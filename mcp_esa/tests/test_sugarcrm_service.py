"""Unit tests for SugarCRMService. Uses mocked httpx to avoid real API calls."""
import time
import pytest
from unittest.mock import MagicMock, patch

from mcp_esa.services.sugarcrm_service import (
    SugarCRMService, SugarCRMConfig, SugarCRMAPIError
)


def _make_config():
    return SugarCRMConfig(
        url="https://sugar.example.com",
        username="u",
        password="p",
        client_id="sugar",
        client_secret="",
        platform="mcp_esa",
        timeout=5,
    )


def _mock_response(json_body, status=200):
    r = MagicMock()
    r.status_code = status
    r.json.return_value = json_body
    r.text = str(json_body)
    # content must be bytes-like so `not resp.content` in _request behaves correctly
    r.content = b"" if status == 204 else b"{}"
    return r


def test_ensure_token_calls_oauth_and_caches():
    svc = SugarCRMService(_make_config())
    with patch.object(svc._client, 'post') as post:
        post.return_value = _mock_response({
            "access_token": "AT", "expires_in": 3600, "refresh_token": "RT"
        })
        svc._ensure_token()
        svc._ensure_token()  # cached, should not call again
    assert post.call_count == 1
    assert svc._access_token == "AT"


def _service_with_token():
    svc = SugarCRMService(_make_config())
    svc._access_token = "AT"
    svc._token_expires_at = time.time() + 3600
    return svc


def test_get_record_hits_correct_path():
    svc = _service_with_token()
    with patch.object(svc._client, 'request') as req:
        req.return_value = _mock_response({"id": "abc", "name": "Acme"})
        out = svc.get_record("Accounts", "abc", fields=["name"])
    args, kwargs = req.call_args
    assert args[0] == "GET"
    assert args[1].endswith("/Accounts/abc")
    assert kwargs["params"] == {"fields": "name"}
    assert out["name"] == "Acme"


def test_list_records_passes_filter_and_paging():
    svc = _service_with_token()
    with patch.object(svc._client, 'request') as req:
        req.return_value = _mock_response({"records": [], "next_offset": -1})
        svc.list_records("Leads", filter=[{"status": "New"}], limit=50, offset=0,
                         fields=["first_name", "last_name"], order_by="date_entered:desc")
    args, kwargs = req.call_args
    assert args[0] == "POST"
    assert args[1].endswith("/Leads/filter")
    p = kwargs["params"]
    assert p["max_num"] == 50
    assert p["offset"] == 0
    assert p["fields"] == "first_name,last_name"
    assert p["order_by"] == "date_entered:desc"
    assert kwargs["json"]["filter"] == [{"status": "New"}]
    assert kwargs["json"]["deleted"] is False


def test_create_record_posts_json():
    svc = _service_with_token()
    with patch.object(svc._client, 'request') as req:
        req.return_value = _mock_response({"id": "new1"})
        out = svc.create_record("Contacts", {"first_name": "A"})
    args, kwargs = req.call_args
    assert args[0] == "POST"
    assert args[1].endswith("/Contacts")
    assert kwargs["json"] == {"first_name": "A"}
    assert out["id"] == "new1"


def test_delete_record_issues_delete():
    svc = _service_with_token()
    with patch.object(svc._client, 'request') as req:
        req.return_value = _mock_response({}, status=204)
        svc.delete_record("Leads", "L1")
    args, _ = req.call_args
    assert args[0] == "DELETE"
    assert args[1].endswith("/Leads/L1")


def test_search_uses_global_endpoint():
    svc = _service_with_token()
    with patch.object(svc._client, 'request') as req:
        req.return_value = _mock_response({"records": []})
        svc.search("Contacts", q="john@example.com", limit=10)
    _, kwargs = req.call_args
    p = kwargs["params"]
    assert p["q"] == "john@example.com"
    assert p["module_list"] == "Contacts"
    assert p["max_num"] == 10


def test_validate_module_rejects_bad_names():
    svc = _service_with_token()
    for bad in ["", "Acc/ounts", "../etc", "Accounts;DROP", "  "]:
        with pytest.raises(SugarCRMAPIError) as e:
            svc.get_record(bad, "id1")
        assert e.value.code == "bad_module"


def test_validate_id_rejects_bad_ids():
    svc = _service_with_token()
    for bad in ["", "../x", "a/b", "a;b"]:
        with pytest.raises(SugarCRMAPIError) as e:
            svc.get_record("Accounts", bad)
        assert e.value.code == "bad_id"


def test_get_related_uses_link_path():
    svc = _service_with_token()
    with patch.object(svc._client, 'request') as req:
        req.return_value = _mock_response({"records": []})
        svc.get_related("Accounts", "A1", "contacts", limit=5)
    args, kwargs = req.call_args
    assert args[0] == "GET"
    assert args[1].endswith("/Accounts/A1/link/contacts")
    assert kwargs["params"]["max_num"] == 5


def test_link_records_posts_related_id():
    svc = _service_with_token()
    with patch.object(svc._client, 'request') as req:
        req.return_value = _mock_response({})
        svc.link_records("Accounts", "A1", "contacts", "C1")
    args, kwargs = req.call_args
    assert args[0] == "POST"
    assert args[1].endswith("/Accounts/A1/link/contacts/C1")


def test_unlink_records_deletes():
    svc = _service_with_token()
    with patch.object(svc._client, 'request') as req:
        req.return_value = _mock_response({})
        svc.unlink_records("Accounts", "A1", "contacts", "C1")
    args, _ = req.call_args
    assert args[0] == "DELETE"
    assert args[1].endswith("/Accounts/A1/link/contacts/C1")


def test_link_name_validation_rejects_bad_names():
    svc = _service_with_token()
    for bad in ["", "con/tacts", "../x", "a;b"]:
        with pytest.raises(SugarCRMAPIError) as e:
            svc.get_related("Accounts", "A1", bad)
        assert e.value.code == "bad_link"


def test_list_modules_hits_metadata():
    svc = _service_with_token()
    with patch.object(svc._client, 'request') as req:
        req.return_value = _mock_response({"modules": {"Accounts": {}}})
        svc.list_modules()
    args, kwargs = req.call_args
    assert args[0] == "GET"
    assert args[1].endswith("/metadata")
    assert kwargs["params"]["type_filter"] == "modules"


def test_list_fields_hits_module_metadata():
    svc = _service_with_token()
    with patch.object(svc._client, 'request') as req:
        req.return_value = _mock_response({"fields": {}})
        svc.list_fields("Accounts")
    args, _ = req.call_args
    assert args[1].endswith("/metadata/modules/Accounts")


def test_create_field_posts_spec():
    svc = _service_with_token()
    spec = {"name": "c_score_c", "type": "int", "label": "Score"}
    with patch.object(svc._client, 'request') as req:
        req.return_value = _mock_response({"name": "c_score_c"})
        svc.create_field("Leads", spec)
    args, kwargs = req.call_args
    assert args[0] == "POST"
    assert args[1].endswith("/Administration/fields/Leads")
    assert kwargs["json"] == spec


def test_create_field_rejects_bad_spec():
    svc = _service_with_token()
    with pytest.raises(SugarCRMAPIError) as e:
        svc.create_field("Leads", {"name": "x"})  # missing type
    assert e.value.code == "bad_spec"


def test_update_dropdown_validates_values_list():
    svc = _service_with_token()
    with pytest.raises(SugarCRMAPIError) as e:
        svc.update_dropdown("my_dd", "not-a-list")
    assert e.value.code == "bad_values"


def test_create_relationship_requires_all_keys():
    svc = _service_with_token()
    with pytest.raises(SugarCRMAPIError) as e:
        svc.create_relationship({"lhs_module": "Accounts"})
    assert e.value.code == "bad_spec"


def test_studio_deploy_calls_rebuild():
    svc = _service_with_token()
    with patch.object(svc._client, 'request') as req:
        req.return_value = _mock_response({"success": True})
        svc.studio_deploy()
    args, _ = req.call_args
    assert args[0] == "POST"
    assert args[1].endswith("/Administration/rebuild")
