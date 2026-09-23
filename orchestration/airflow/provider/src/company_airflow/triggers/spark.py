"""Wait for Spark without holding an Airflow worker slot."""
import asyncio
import time

from airflow.triggers.base import BaseTrigger, TriggerEvent

from company_airflow.hooks.spark_kubernetes import SparkApplicationHook
from company_airflow.status import application_result


class SparkApplicationTrigger(BaseTrigger):
    def __init__(self, *, name, namespace, deadline, poll_interval=10,
                 in_cluster=True, kube_context=None):
        super().__init__()
        self.name = name
        self.namespace = namespace
        self.deadline = deadline
        self.poll_interval = poll_interval
        self.in_cluster = in_cluster
        self.kube_context = kube_context

    def serialize(self):
        return ("company_airflow.triggers.spark.SparkApplicationTrigger", {
            "name": self.name, "namespace": self.namespace, "deadline": self.deadline,
            "poll_interval": self.poll_interval, "in_cluster": self.in_cluster,
            "kube_context": self.kube_context,
        })

    async def run(self):
        hook = await asyncio.to_thread(SparkApplicationHook, self.namespace, self.in_cluster, self.kube_context)
        errors = 0
        while time.time() < self.deadline:
            try:
                app = await asyncio.to_thread(hook.get, self.name)
                errors = 0
                result = application_result(app)
                if result:
                    yield TriggerEvent({"name": self.name, **result})
                    return
            except Exception as error:
                errors += 1
                self.log.warning("Spark state lookup failed (%s/6): %s", errors, type(error).__name__)
                if errors >= 6:
                    yield TriggerEvent({"name": self.name, "status": "error", "state": "MonitoringFailed",
                                        "message": f"Cannot query Kubernetes: {type(error).__name__}"})
                    return
            await asyncio.sleep(self.poll_interval)
        # Deadline is persisted in trigger arguments, so a triggerer restart cannot reset it.
        try:
            await asyncio.to_thread(hook.delete, self.name)
            message = "Spark execution timed out; application deletion requested"
        except Exception as error:
            message = f"Spark execution timed out; cleanup failed: {type(error).__name__}"
        yield TriggerEvent({"name": self.name, "status": "error", "state": "TimedOut", "message": message})
