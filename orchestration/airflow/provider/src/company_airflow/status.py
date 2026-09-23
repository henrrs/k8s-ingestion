"""Apache spark.apache.org/v1 status, deliberately independent of Kubeflow APIs."""

FAILURES = {
    "Failed", "DriverStartTimedOut", "DriverReadyTimedOut", "ExecutorsStartTimedOut",
    "SchedulingFailure", "DriverEvicted",
}
TERMINATED = {"ResourceReleased", "TerminatedWithoutReleaseResources"}


def application_result(application: dict) -> dict | None:
    """Return a terminal result, or None. Resource cleanup alone is never success.

    Operator retries are disabled in our manifests. Airflow owns retries, so any
    observed failure is definitive for this SparkApplication.
    """
    status = application.get("status") or {}
    current = status.get("currentState") or {}
    state = current.get("currentStateSummary", "Submitted")
    if state == "Succeeded" or state in FAILURES:
        return {"status": "success" if state == "Succeeded" else "error",
                "state": state, "message": current.get("message", "")}
    if state not in TERMINATED:
        return None
    history = status.get("stateTransitionHistory") or {}
    for _, event in sorted(history.items(), key=lambda item: int(item[0]), reverse=True):
        previous = event.get("currentStateSummary")
        if previous == "Succeeded" or previous in FAILURES:
            return {"status": "success" if previous == "Succeeded" else "error",
                    "state": previous, "message": event.get("message", "")}
    return {"status": "error", "state": state,
            "message": "Application terminated without a preserved success/failure state"}
