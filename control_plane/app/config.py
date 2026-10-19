"""Environment-based configuration for the control plane.

Set env vars to override defaults:
    export MPRPC_DB_URL="mysql+pymysql://user:pass@localhost/mprpc_ee"
    export MPRPC_ZK_HOSTS="127.0.0.1:2181"
    export MPRPC_PROMETHEUS_URL="http://localhost:9090"
    export MPRPC_GRAFANA_URL="http://localhost:3000"
"""

import os


DATABASE_URL = os.getenv(
    "MPRPC_DB_URL",
    "sqlite:///control_plane.db",  # default fallback
)

ZK_HOSTS = os.getenv("MPRPC_ZK_HOSTS", "127.0.0.1:2181")

PROMETHEUS_URL = os.getenv("MPRPC_PROMETHEUS_URL", "http://localhost:9090")

GRAFANA_URL = os.getenv("MPRPC_GRAFANA_URL", "http://localhost:3000")

ALERTMANAGER_URL = os.getenv("MPRPC_ALERTMANAGER_URL", "http://localhost:9093")

REDIS_HOST = os.getenv("MPRPC_REDIS_HOST", "127.0.0.1")
REDIS_PORT = int(os.getenv("MPRPC_REDIS_PORT", "6379"))

TRACE_LOG_DIR = os.getenv("MPRPC_TRACE_LOG_DIR", os.path.join(os.path.dirname(__file__), "..", "..", "bin"))
