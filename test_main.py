import importlib.util
import sys
import types
import unittest
from pathlib import Path


def load_main_module():
    google = types.ModuleType("google")
    google_cloud = types.ModuleType("google.cloud")
    google_cloud.bigquery = types.ModuleType("google.cloud.bigquery")
    google_cloud.pubsub_v1 = types.ModuleType("google.cloud.pubsub_v1")
    google_oauth2 = types.ModuleType("google.oauth2")
    service_account = types.ModuleType("google.oauth2.service_account")
    service_account.Credentials = types.SimpleNamespace(from_service_account_info=lambda *args, **kwargs: None)

    flask = types.ModuleType("flask")
    flask.Request = object
    flask.jsonify = lambda payload: payload

    functions_framework = types.ModuleType("functions_framework")
    functions_framework.http = lambda func: func

    sys.modules.update({
        "google": google,
        "google.cloud": google_cloud,
        "google.cloud.bigquery": google_cloud.bigquery,
        "google.cloud.pubsub_v1": google_cloud.pubsub_v1,
        "google.oauth2": google_oauth2,
        "google.oauth2.service_account": service_account,
        "flask": flask,
        "functions_framework": functions_framework,
    })

    spec = importlib.util.spec_from_file_location("rico_main", Path(__file__).with_name("main.py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class LeadIdPayloadTests(unittest.TestCase):
    def test_adds_lead_id_inside_notes_data(self):
        module = load_main_module()
        payload = {"status": True, "data": {"notes": []}, "message": "Done"}

        result = module.add_lead_id_to_payload("12345", "notes", payload)

        self.assertEqual(result["data"], {"lead_id": "12345", "notes": []})

    def test_adds_lead_id_inside_tasks_data(self):
        module = load_main_module()
        payload = {"status": True, "data": {"tasks": []}, "message": "Done"}

        result = module.add_lead_id_to_payload("12345", "tasks", payload)

        self.assertEqual(result["data"], {"lead_id": "12345", "tasks": []})

    def test_leaves_other_endpoints_unchanged(self):
        module = load_main_module()
        payload = {"status": True, "data": {"status_history": []}, "message": "Done"}

        result = module.add_lead_id_to_payload("12345", "status-history", payload)

        self.assertEqual(result, payload)


class IdentifierValidationTests(unittest.TestCase):
    def test_accepts_valid_table_name(self):
        module = load_main_module()

        self.assertTrue(module._is_safe_identifier(
            "kelly-agency-production.silver.tb_ka_ricochet_leads_download_extracts",
            module.TABLE_NAME_PATTERN,
        ))

    def test_accepts_valid_field_name(self):
        module = load_main_module()

        self.assertTrue(module._is_safe_identifier("lead_status", module.FIELD_NAME_PATTERN))

    def test_rejects_table_name_with_injection_attempt(self):
        module = load_main_module()

        self.assertFalse(module._is_safe_identifier("x; DROP TABLE y", module.TABLE_NAME_PATTERN))

    def test_rejects_field_name_with_injection_attempt(self):
        module = load_main_module()

        self.assertFalse(module._is_safe_identifier("lead_status OR 1=1", module.FIELD_NAME_PATTERN))

    def test_rejects_non_string_values(self):
        module = load_main_module()

        self.assertFalse(module._is_safe_identifier(None, module.TABLE_NAME_PATTERN))


class CustomLeadsQueryTests(unittest.TestCase):
    def test_builds_filtered_query_with_parameter_placeholder(self):
        module = load_main_module()

        query = module.Config.get_custom_leads_query(
            "kelly-agency-production.silver.tb_ka_ricochet_leads_download_extracts",
            "lead_status",
        )

        self.assertIn("FROM", query)
        self.assertIn("kelly-agency-production.silver.tb_ka_ricochet_leads_download_extracts", query)
        self.assertIn("lead_status IN UNNEST(@filter_value)", query)


if __name__ == "__main__":
    unittest.main()