"""Validated execution contract. Connection secrets are environment references."""

from dataclasses import dataclass, field
import re

from company_ingestion_core.config import PlannerConfig


@dataclass(frozen=True)
class SourceConfig:
    type: str
    host: str
    database: str
    schema: str
    table: str
    port: int = 1433
    user_env: str = "SQLSERVER_USER"
    password_env: str = "SQLSERVER_PASSWORD"
    encrypt: bool = True
    trust_server_certificate: bool = False
    query_timeout_seconds: int = 600

    def __post_init__(self):
        if self.type != "sqlserver":
            raise ValueError(f"Unsupported source technology: {self.type}")
        for name in ("host", "database", "schema", "table", "user_env", "password_env"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value or "\x00" in value:
                raise ValueError(f"source.{name} must be a non-empty string")
        if any(char in self.host for char in ";{}\r\n"):
            raise ValueError("source.host contains invalid JDBC URL characters")
        if not isinstance(self.port, int) or not 1 <= self.port <= 65535:
            raise ValueError("source.port must be between 1 and 65535")
        if not isinstance(self.query_timeout_seconds, int) or self.query_timeout_seconds < 1:
            raise ValueError("source.query_timeout_seconds must be positive")


@dataclass(frozen=True)
class DestinationConfig:
    uri: str
    format: str = "delta"

    def __post_init__(self):
        if self.format != "delta":
            raise ValueError("This engine version supports destination.format=delta")
        if not isinstance(self.uri, str) or not self.uri:
            raise ValueError("destination.uri is required")


@dataclass(frozen=True)
class MetricsConfig:
    output_uri: str = "s3a://metrics/runs"
    pushgateway: str | None = None


@dataclass(frozen=True)
class ExecutionConfig:
    run_id: str
    profile: str
    source: SourceConfig
    destination: DestinationConfig
    planner: PlannerConfig = field(default_factory=PlannerConfig)
    metrics: MetricsConfig = field(default_factory=MetricsConfig)

    def __post_init__(self):
        if not isinstance(self.run_id, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,199}", self.run_id):
            raise ValueError("run_id must be a safe, unique path segment (1–200 characters)")
        if not isinstance(self.profile, str) or not self.profile:
            raise ValueError("profile must be a non-empty string")

    @property
    def run_uri(self):
        return f"{self.destination.uri.rstrip('/')}/{self.run_id}"

    @classmethod
    def from_dict(cls, data):
        return cls(
            run_id=data["run_id"],
            profile=data["profile"],
            source=SourceConfig(**data["source"]),
            destination=DestinationConfig(**data["destination"]),
            planner=PlannerConfig(**data.get("planner", {})),
            metrics=MetricsConfig(**data.get("metrics", {})),
        )
