from fastapi.testclient import TestClient

from etloom.web.app import app

client = TestClient(app)


def test_health_endpoint():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"


def test_dashboard_renders_pipeline():
    response = client.get("/")
    assert response.status_code == 200
    assert "Clean Transactions" in response.text
    assert "ETLoom" in response.text


def test_pipeline_editor_renders_yaml():
    response = client.get("/pipelines/clean_transactions")
    assert response.status_code == 200
    assert "Pipeline YAML" in response.text
    assert "source_has_records" in response.text


def test_validation_api():
    response = client.get("/api/pipelines/clean_transactions")
    content = response.json()["yaml_content"]
    validation = client.post("/api/pipelines/validate", json={"yaml_content": content})
    assert validation.status_code == 200
    assert validation.json()["valid"] is True
