"""Wait for a Kubernetes Job without holding an Airflow worker slot."""
import asyncio
import time

from airflow.triggers.base import BaseTrigger, TriggerEvent

from company_airflow.hooks.kubernetes_job import KubernetesJobHook
from company_airflow.job import job_result


class KubernetesJobTrigger(BaseTrigger):
    def __init__(self, *, name, namespace, deadline, poll_interval=5,
                 in_cluster=True, kube_context=None):
        super().__init__()
        self.name, self.namespace, self.deadline = name, namespace, deadline
        self.poll_interval, self.in_cluster, self.kube_context = poll_interval, in_cluster, kube_context

    def serialize(self):
        return ("company_airflow.triggers.job.KubernetesJobTrigger", {
            "name": self.name, "namespace": self.namespace, "deadline": self.deadline,
            "poll_interval": self.poll_interval, "in_cluster": self.in_cluster,
            "kube_context": self.kube_context,
        })

    async def run(self):
        hook = await asyncio.to_thread(KubernetesJobHook, self.namespace, self.in_cluster, self.kube_context)
        errors = 0
        while time.time() < self.deadline:
            try:
                result = job_result(await asyncio.to_thread(hook.get, self.name))
                errors = 0
                if result:
                    yield TriggerEvent({"name": self.name, **result})
                    return
            except Exception as error:
                errors += 1
                self.log.warning("Job state lookup failed (%s/6): %s", errors, type(error).__name__)
                if errors >= 6:
                    yield TriggerEvent({"name": self.name, "status": "error", "state": "MonitoringFailed",
                                        "message": f"Cannot query Kubernetes: {type(error).__name__}"})
                    return
            await asyncio.sleep(self.poll_interval)
        try:
            await asyncio.to_thread(hook.delete, self.name)
            message = "Kubernetes Job timed out; deletion requested"
        except Exception as error:
            message = f"Kubernetes Job timed out; cleanup failed: {type(error).__name__}"
        yield TriggerEvent({"name": self.name, "status": "error", "state": "TimedOut", "message": message})
