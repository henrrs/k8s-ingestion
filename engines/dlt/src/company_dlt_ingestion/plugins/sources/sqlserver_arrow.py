"""Native Arrow streaming from SQL Server for the distributed data plane."""

import os


def odbc_value(value):
    """Quote an ODBC connection-string value without exposing it elsewhere."""
    return "{" + str(value).replace("}", "}}") + "}"


def connection_string(source):
    user = os.environ[source.get("user_env", "SQLSERVER_USER")]
    password = os.environ[source.get("password_env", "SQLSERVER_PASSWORD")]
    server = f"{source['host']},{int(source.get('port', 1433))}"
    encrypt = "yes" if source.get("encrypt", False) else "no"
    trust = "yes" if source.get("trust_server_certificate", True) else "no"
    return ";".join(
        [
            f"Server={odbc_value(server)}",
            f"Database={odbc_value(source['database'])}",
            f"UID={odbc_value(user)}",
            f"PWD={odbc_value(password)}",
            f"Encrypt={encrypt}",
            f"TrustServerCertificate={trust}",
        ]
    )


def connect(source):
    import mssql_python

    return mssql_python.connect(connection_string(source))
