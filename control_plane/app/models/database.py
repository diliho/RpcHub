"""Database models for the control plane.

Uses SQLAlchemy with configurable backend (SQLite default, MySQL for production).
Set MPRPC_DB_URL env var to switch to MySQL:
    export MPRPC_DB_URL="mysql+pymysql://user:pass@localhost/mprpc_ee"
"""

import os
from datetime import datetime
from sqlalchemy import (
    Column, Integer, String, Float, Boolean, DateTime, Text, JSON,
    create_engine, Index,
)
from sqlalchemy.orm import declarative_base, sessionmaker

Base = declarative_base()

_db_url = os.getenv("MPRPC_DB_URL", "sqlite:///control_plane.db")
connect_args = {}
if _db_url.startswith("sqlite"):
    connect_args["check_same_thread"] = False

engine = create_engine(_db_url, echo=False, future=True, connect_args=connect_args)
SessionLocal = sessionmaker(bind=engine, future=True)


class Node(Base):
    """RPC provider node."""
    __tablename__ = "nodes"

    id = Column(Integer, primary_key=True, autoincrement=True)
    ip = Column(String(45), nullable=False)
    port = Column(Integer, nullable=False)
    hostname = Column(String(255), default="")
    status = Column(String(20), default="unknown")  # online/offline/healthy/unhealthy
    rpc_port = Column(Integer, default=0)
    region = Column(String(100), default="default")
    labels = Column(JSON, default=dict)
    last_heartbeat = Column(DateTime, default=None)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        Index("idx_node_ip_port", "ip", "port", unique=True),
        Index("idx_node_status", "status"),
    )


class Service(Base):
    """Registered RPC service."""
    __tablename__ = "services"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), nullable=False, unique=True)
    description = Column(Text, default="")
    owner = Column(String(100), default="")
    version = Column(String(50), default="1.0.0")
    methods = Column(JSON, default=list)  # list of method names
    status = Column(String(20), default="active")
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class ServiceInstance(Base):
    """Instance of a service on a specific node."""
    __tablename__ = "service_instances"

    id = Column(Integer, primary_key=True, autoincrement=True)
    service_name = Column(String(255), nullable=False)
    node_ip = Column(String(45), nullable=False)
    node_port = Column(Integer, nullable=False)
    weight = Column(Integer, default=1)
    status = Column(String(20), default="online")  # online/offline
    created_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        Index("idx_svc_inst", "service_name", "node_ip", "node_port", unique=True),
    )


class ConfigVersion(Base):
    """Configuration version record."""
    __tablename__ = "config_versions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    scope = Column(String(50), nullable=False)  # global / service / method
    scope_key = Column(String(255), default="")  # service name or "service/method"
    key = Column(String(255), nullable=False)
    value = Column(Text, nullable=False)
    version = Column(Integer, nullable=False)
    status = Column(String(20), default="pending")  # pending / applied / rolled_back
    created_by = Column(String(100), default="admin")
    created_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        Index("idx_config_scope", "scope", "scope_key", "key"),
        Index("idx_config_version", "scope", "scope_key", "key", "version"),
    )


class RateLimitRule(Base):
    """Rate limiting rule for a service/method."""
    __tablename__ = "rate_limit_rules"

    id = Column(Integer, primary_key=True, autoincrement=True)
    service_name = Column(String(255), nullable=False, default="*")
    method_name = Column(String(255), nullable=False, default="*")
    max_qps = Column(Integer, nullable=False, default=1000)
    algorithm = Column(String(50), default="token_bucket")
    enabled = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        Index("idx_ratelimit_svc", "service_name", "method_name"),
    )


class CircuitBreakerRule(Base):
    """Circuit breaker rule for a service/method."""
    __tablename__ = "circuit_breaker_rules"

    id = Column(Integer, primary_key=True, autoincrement=True)
    service_name = Column(String(255), nullable=False, default="*")
    method_name = Column(String(255), nullable=False, default="*")
    failure_threshold = Column(Integer, default=5)
    timeout_sec = Column(Integer, default=30)
    enabled = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class AuditLog(Base):
    """Audit log for control plane operations."""
    __tablename__ = "audit_logs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    action = Column(String(100), nullable=False)
    resource_type = Column(String(50), nullable=False)
    resource_id = Column(String(255), default="")
    detail = Column(JSON, default=dict)
    operator = Column(String(100), default="admin")
    created_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (Index("idx_audit_action", "action"),)


class TaskRecord(Base):
    """TaskHub 任务终态记录（对账凭证 + 大盘最近列表）。

    数据来自 task:result Stream（task_persister 后台消费），
    时间字段保留 provider 侧毫秒纪元，展示层负责格式化。
    """
    __tablename__ = "task_records"

    id = Column(Integer, primary_key=True, autoincrement=True)
    task_id = Column(String(64), nullable=False)
    type = Column(String(20), default="0")          # 0=sleep 1=compute 2=fail
    state = Column(String(20), nullable=False)      # PENDING/RUNNING/SUCCEEDED/FAILED/DEAD/CANCELED
    priority = Column(String(4), default="1")
    retries = Column(Integer, default=0)
    worker = Column(String(128), default="")
    submitted_at = Column(Integer, default=0)       # ms epoch
    started_at = Column(Integer, default=0)
    finished_at = Column(Integer, default=0)
    result_payload = Column(Text, default="")
    fail_reason = Column(Text, default="")

    __table_args__ = (
        Index("idx_task_taskid", "task_id", unique=True),
        Index("idx_task_state", "state"),
        Index("idx_task_finished", "finished_at"),
    )


def init_db():
    """Create all tables."""
    Base.metadata.create_all(bind=engine)
