import pytest
from company_airflow.application import build_application, execution_name
from company_airflow.status import application_result

def test_resource_cleanup_is_not_success():
    result=application_result({"status":{"currentState":{"currentStateSummary":"ResourceReleased"}}})
    assert result["status"]=="error"

@pytest.mark.parametrize("state,result",[("Succeeded","success"),("Failed","error")])
def test_terminal_state_survives_resource_cleanup(state,result):
    app={"status":{"currentState":{"currentStateSummary":"ResourceReleased"},"stateTransitionHistory":{"3":{"currentStateSummary":state}}}}
    assert application_result(app)["status"]==result

def test_names_reattach_same_attempt_but_separate_retries():
    name=execution_name("dag","task","run",1)
    assert name==execution_name("dag","task","run",1)
    assert name!=execution_name("dag","task","run",2)
    assert len(name)<=63

def test_native_apache_manifest_keeps_secrets_out_of_parameters():
    params={"source":{"type":"sqlserver","table":"small"}}
    app=build_application(name="test",namespace="spark-lab",runtime="runtime:1",spark_version="4.2.0",wheel_url="http://artifacts/motor.whl",parameters=params,compute_profile="medium")
    assert app["apiVersion"]=="spark.apache.org/v1"
    assert app["spec"]["sparkConf"]["spark.executor.cores"]=="2"
    assert "run_id" not in params
    env=app["spec"]["driverSpec"]["podTemplateSpec"]["spec"]["containers"][0]["env"]
    credential=next(e for e in env if e["name"]=="SQLSERVER_PASSWORD")
    assert "valueFrom" in credential and "value" not in credential
