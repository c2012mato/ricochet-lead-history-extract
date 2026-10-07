import os
import json
import re
import time
import collections
import threading
import requests
from google.cloud import bigquery
from google.cloud import pubsub_v1
from google.oauth2 import service_account
from flask import Request, jsonify
import functions_framework
from datetime import datetime
import pytz
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def add_lead_id_to_payload(lead_id: str, endpoint: str, payload: dict) -> dict:
    if endpoint not in {"notes", "tasks"}:
        return payload

    data = payload.get("data")
    if not isinstance(data, dict):
        return payload

    payload["data"] = {"lead_id": lead_id, **data}
    return payload


# BigQuery can't bind identifiers as query params, so table/field names are allow-listed instead.
TABLE_NAME_PATTERN = re.compile(r"^[A-Za-z0-9_.\-]+$")
FIELD_NAME_PATTERN = re.compile(r"^[A-Za-z0-9_]+$")


def _is_safe_identifier(value, pattern: re.Pattern) -> bool:
    return isinstance(value, str) and bool(pattern.fullmatch(value))

# --- Config ---
class Config:
    PROJECT_ID = os.environ.get("PROJECT_ID")
    TOPIC_ID = os.environ.get("PUBSUB_TOPIC")
    SERVICE_ACCOUNT_INFO = os.environ.get("SERVICE_ACCOUNT_INFO")
    BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", "python_extracts")
    ENDPOINTS = {
        "status-history": "tb_rico_status_leads",
        "call-history": "tb_rico_status_calls",
        "ma-status-history": "tb_ma_rico_status_leads",
        "ma-call-history": "tb_ma_rico_status_calls",
        "notes": "tb_rico_notes",
        "ma-notes": "tb_ma_rico_notes",
        "tasks": "tb_rico_tasks",
        "ma-tasks": "tb_ma_rico_tasks",
        "leads": "tb_rico_leads",
        "ma-leads": "tb_ma_rico_leads"
    }
    MAX_WORKERS = 20
    RATE_LIMIT_REQUESTS = 7000
    RATE_LIMIT_WINDOW_SECONDS = 300
    
    @staticmethod
    def get_yesterday_called_leads_query(agency: str) -> str:
        """Generate the query for yesterday's called leads based on agency."""
        table_name = "tb_ricochet_download_extracts"
        if agency and agency.upper() == "MA":
            table_name = "tb_ma_ricochet_download_extracts"
        
        return f"""
        SELECT DISTINCT 
            lead_id
        FROM 
            {Config.PROJECT_ID}.{Config.BIGQUERY_DATASET}.{table_name}
        WHERE 
        DATE(call_date) = DATE_SUB(CURRENT_DATE('America/New_York'), INTERVAL 1 DAY)
    """

    @staticmethod
    def get_custom_leads_query(custom_table: str, filter_field: str) -> str:
        """Build a filtered lead query; caller must validate custom_table/filter_field first."""
        return f"""
        SELECT DISTINCT
            lead_id
        FROM
            {custom_table}
        WHERE
            {filter_field} IN UNNEST(@filter_value)
    """


# --- BigQueryClient ---
class BigQueryClient:
    def __init__(self, project_id):
        self.project_id = project_id
        sa_info = Config.SERVICE_ACCOUNT_INFO
        if sa_info:
            sa_dict = json.loads(sa_info)
            credentials = service_account.Credentials.from_service_account_info(
                sa_dict,
                scopes=["https://www.googleapis.com/auth/bigquery"],
            )
            self.client = bigquery.Client(project=project_id, credentials=credentials)
        else:
            self.client = bigquery.Client(project=project_id)

    def run_query(self, query, job_config=None):
        query_job = self.client.query(query, job_config=job_config)
        results = query_job.result()
        return results.to_dataframe()


# --- RateLimiter ---
class RateLimiter:
    """
    Thread-safe sliding-window rate limiter.
    Allows at most `max_requests` calls within any `window_seconds` window.
    Callers block in acquire() until a slot is available.
    """
    def __init__(self, max_requests: int, window_seconds: float):
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self._lock = threading.Lock()
        self._timestamps = collections.deque()

    def acquire(self):
        while True:
            with self._lock:
                now = time.monotonic()
                # Drop timestamps that have fallen outside the window
                while self._timestamps and self._timestamps[0] <= now - self.window_seconds:
                    self._timestamps.popleft()

                if len(self._timestamps) < self.max_requests:
                    self._timestamps.append(now)
                    return  # slot acquired

                # Calculate how long to wait until the oldest request leaves the window
                sleep_for = self.window_seconds - (now - self._timestamps[0])

            # Sleep outside the lock so other threads aren't blocked
            logger.info(f"Rate limit reached ({self.max_requests} req/{self.window_seconds}s). "
                        f"Sleeping {sleep_for:.1f}s...")
            time.sleep(max(sleep_for, 0))


# --- StatusHistoryPipeline ---
class StatusHistoryPipeline:
    def __init__(self, project_id: str, topic_id: str, ricochet_host_name: str, ricochet_user_id: str, agency: str):
        self.bq_client = BigQueryClient(project_id)
        self.publisher = pubsub_v1.PublisherClient()
        self.topic_path = self.publisher.topic_path(project_id, topic_id)
        self.ricochet_host_name = ricochet_host_name
        self.ricochet_user_id = ricochet_user_id
        self.agency = agency

    def fetch_lead_ids(self, backfill_query: str = None, custom_leads_config: dict = None) -> list:
        if custom_leads_config:
            logger.info("Fetching custom filtered lead IDs from BigQuery...")
            query = Config.get_custom_leads_query(
                custom_leads_config["custom_table"], custom_leads_config["filter_field"]
            )
            job_config = bigquery.QueryJobConfig(query_parameters=[
                bigquery.ArrayQueryParameter("filter_value", "STRING", custom_leads_config["filter_value"])
            ])
            df = self.bq_client.run_query(query, job_config=job_config)
        elif backfill_query:
            logger.info("Fetching backfill lead IDs from BigQuery...")
            df = self.bq_client.run_query(backfill_query)
        else:
            logger.info("Fetching yesterday's called lead IDs from BigQuery...")
            query = Config.get_yesterday_called_leads_query(self.agency)
            df = self.bq_client.run_query(query)
        if df is None or df.empty:
            logger.warning("No lead IDs returned from BigQuery.")
            return []
        lead_ids = df["lead_id"].astype(str).tolist()
        logger.info(f"Fetched {len(lead_ids)} lead IDs.")
        return lead_ids

    def _fetch_single_lead(self, lead_id: str, endpoint: str, report_name: str, rate_limiter: RateLimiter) -> dict:
        """GET a single lead endpoint. Thread-safe."""
        rate_limiter.acquire()

        url = f"{self.ricochet_host_name}/{endpoint}/{lead_id}" if 'rico_leads' in report_name else f"{self.ricochet_host_name}/{lead_id}/{endpoint}"
        headers = {
            "Content-Type": "application/json",
            "X-Auth-Token": self.ricochet_user_id,
        }

        try:
            response = requests.get(url, headers=headers, timeout=30)
            if response.status_code in (200, 201):
                data = add_lead_id_to_payload(lead_id, endpoint, response.json())
                return {
                    "lead_id": lead_id,
                    "report_name": report_name,
                    "success": True,
                    "data": data,
                    "status_code": response.status_code,
                }
            else:
                return {
                    "lead_id": lead_id,
                    "report_name": report_name,
                    "success": False,
                    "error": response.text,
                    "status_code": response.status_code,
                }
        except Exception as e:
            return {
                "lead_id": lead_id,
                "report_name": report_name,
                "success": False,
                "error": str(e),
                "status_code": None,
            }

    def fetch_all(self, lead_ids: list, endpoint: str, report_name: str) -> list:
        """Fire concurrent GET requests for all lead IDs for a single endpoint, respecting rate limits."""
        results = []
        rate_limiter = RateLimiter(Config.RATE_LIMIT_REQUESTS, Config.RATE_LIMIT_WINDOW_SECONDS)

        logger.info(f"Submitting {len(lead_ids)} leads for endpoint '{endpoint}' to ThreadPoolExecutor "
                    f"(max_workers={Config.MAX_WORKERS})...")

        with ThreadPoolExecutor(max_workers=Config.MAX_WORKERS) as executor:
            future_to_lead = {
                executor.submit(self._fetch_single_lead, lead_id, endpoint, report_name, rate_limiter): lead_id
                for lead_id in lead_ids
            }

            for future in as_completed(future_to_lead):
                result = future.result()
                results.append(result)

                if not result["success"]:
                    logger.warning(
                        f"lead_id={result['lead_id']} failed "
                        f"(status={result['status_code']}): {result.get('error', '')}"
                    )

        return results

    def publish_result(self, lead_id: str, data: dict, report_name: str) -> str:
        """Publish a single lead's API response to Pub/Sub. Returns message_id."""
        eastern = pytz.timezone("America/New_York")
        ingestion_timestamp = datetime.now(eastern).isoformat()
        
        pubsub_data = {
            "report_name": report_name,
            "json_payload": json.dumps(data),
            "ingestion_timestamp": ingestion_timestamp,
        }

        message = json.dumps(pubsub_data).encode("utf-8")
        future = self.publisher.publish(self.topic_path, message)
        return future.result(timeout=30)

    def run(self, endpoint: str, report_name: str, backfill_query: str = None, custom_leads_config: dict = None) -> dict:
        """Orchestrate the full pipeline. Returns a summary dict."""
        lead_ids = self.fetch_lead_ids(backfill_query=backfill_query, custom_leads_config=custom_leads_config)
        if not lead_ids:
            return {
                "total_leads": 0,
                "successful": 0,
                "failed": 0,
                "messages_published": 0,
            }

        api_results = self.fetch_all(lead_ids, endpoint, report_name)

        messages_published = 0
        failed = 0

        for result in api_results:
            if result["success"]:
                try:
                    self.publish_result(result["lead_id"], result["data"], result["report_name"])
                    messages_published += 1
                except Exception as e:
                    logger.error(f"Failed to publish lead_id={result['lead_id']}: {e}", exc_info=True)
                    failed += 1
            else:
                failed += 1

        successful = messages_published
        summary = {
            "total_leads": len(lead_ids),
            "successful": successful,
            "failed": failed,
            "messages_published": messages_published,
        }
        logger.info(f"Pipeline complete: {summary}")
        return summary


# --- Cloud Function Entrypoint ---
@functions_framework.http
def main_handler(request: Request):
    try:
        # Parse JSON payload
        try:
            payload = request.get_json(force=True)
            if not isinstance(payload, dict):
                raise ValueError("Payload must be a JSON object.")
        except Exception as exc:
            logger.error(f"Invalid JSON payload: {exc}")
            return jsonify({"error": "Invalid JSON payload", "details": str(exc)}), 400

        # Validate required environment variables
        required_env_vars = {
            "PROJECT_ID": Config.PROJECT_ID,
            "PUBSUB_TOPIC": Config.TOPIC_ID,
        }

        missing_vars = [k for k, v in required_env_vars.items() if not v]
        if missing_vars:
            error_msg = f"Missing required environment variables: {', '.join(missing_vars)}"
            logger.error(error_msg)
            return jsonify({"error": error_msg}), 500

        # Validate required request params
        agency = payload.get("agency")
        ricochet_host_name = payload.get("ricochet_host_name")
        ricochet_user_id = payload.get("ricochet_user_id")

        missing_fields = [f for f, v in {
            "agency": agency,
            "ricochet_host_name": ricochet_host_name,
            "ricochet_user_id": ricochet_user_id,
        }.items() if not v]
        if missing_fields:
            return jsonify({"error": f"Missing required fields in request body: {', '.join(missing_fields)}"}), 405

        endpoint = payload.get("endpoint")
        endpoint = endpoint.strip() if isinstance(endpoint, str) else None
        if not endpoint:
            return jsonify({"error": "Missing required field 'endpoint' in request body"}), 405

        if endpoint not in Config.ENDPOINTS:
            return jsonify({"error": f"Invalid endpoint: '{endpoint}'. Valid options: {list(Config.ENDPOINTS.keys())}"}), 405

        report_name = Config.ENDPOINTS[endpoint]
        # Strip agency prefix (e.g. "ma-call-history" -> "call-history") for the actual API path
        api_endpoint = endpoint.removeprefix("ka-").removeprefix("ma-")

        # Parse backfill options
        is_back_fill = payload.get("is_back_fill", "no")
        backfill_query = None
        if is_back_fill == "yes":
            back_fill_leads_table = payload.get("back_fill_leads_table")
            if not back_fill_leads_table:
                return jsonify({"error": "Missing required field 'back_fill_leads_table' when is_back_fill is 'yes'"}), 405
            if not _is_safe_identifier(back_fill_leads_table, TABLE_NAME_PATTERN):
                return jsonify({"error": "Invalid 'back_fill_leads_table' format"}), 400
            backfill_query = f"SELECT DISTINCT lead_id FROM `{back_fill_leads_table}`"
            logger.info(f"Backfill mode enabled. Using table: {back_fill_leads_table}")

        # Parse custom leads filter options
        custom_leads = payload.get("custom_leads", False)
        custom_leads_config = None
        if custom_leads is True:
            if is_back_fill == "yes":
                return jsonify({"error": "custom_leads and is_back_fill are mutually exclusive"}), 405

            custom_table = payload.get("custom_table")
            filter_field = payload.get("filter_field")
            filter_value = payload.get("filter_value")

            missing_custom_fields = [f for f, v in {
                "custom_table": custom_table,
                "filter_field": filter_field,
                "filter_value": filter_value,
            }.items() if not v]
            if missing_custom_fields:
                return jsonify({"error": f"Missing required fields in request body: {', '.join(missing_custom_fields)}"}), 405

            if not isinstance(filter_value, list):
                return jsonify({"error": "'filter_value' must be a list"}), 405

            if not _is_safe_identifier(custom_table, TABLE_NAME_PATTERN) or not _is_safe_identifier(filter_field, FIELD_NAME_PATTERN):
                return jsonify({"error": "Invalid 'custom_table' or 'filter_field' format"}), 400

            custom_leads_config = {
                "custom_table": custom_table,
                "filter_field": filter_field,
                "filter_value": filter_value,
            }
            logger.info(f"Custom leads mode enabled. Using table: {custom_table}, filter: {filter_field}")

        logger.info(f"Starting Ricochet extraction pipeline for endpoint: {api_endpoint} (report: {report_name})...")

        pipeline = StatusHistoryPipeline(Config.PROJECT_ID, Config.TOPIC_ID, ricochet_host_name, ricochet_user_id, agency)
        summary = pipeline.run(endpoint=api_endpoint, report_name=report_name, backfill_query=backfill_query, custom_leads_config=custom_leads_config)

        return jsonify({"status": "success", "data": summary}), 200

    except Exception as e:
        logger.error(f"Unhandled exception: {str(e)}", exc_info=True)
        return jsonify({"error": "Internal server error", "details": str(e)}), 500
