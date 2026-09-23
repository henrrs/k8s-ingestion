"""Company provider for Spark and generic Kubernetes batch workloads."""
__version__ = "0.3.0"

def get_provider_info():
    return {
        "package-name": "company-airflow-provider",
        "name": "Company Kubernetes Workloads",
        "description": "Run versioned Spark applications and container workloads on Kubernetes.",
        "versions": [__version__],
        "operators": [{"integration-name": "Company Kubernetes", "python-modules": [
            "company_airflow.operators.spark", "company_airflow.operators.enterprise_k8s",
            "company_airflow.operators.enterprise_ingestion"]}],
    }
