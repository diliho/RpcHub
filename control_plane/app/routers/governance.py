"""Service governance API - rate limit rules, circuit breaker rules."""

import logging
from typing import List, Optional

import redis as redis_lib
from fastapi import APIRouter, HTTPException

from ..config import REDIS_HOST, REDIS_PORT
from ..models.database import SessionLocal, RateLimitRule, CircuitBreakerRule
from ..schemas.schemas import (
    RateLimitRuleCreate, RateLimitRuleResponse, RateLimitRuleUpdate,
    CircuitBreakerRuleCreate, CircuitBreakerRuleResponse, CircuitBreakerRuleUpdate,
)

logger = logging.getLogger("governance")

router = APIRouter(prefix="/api/v1/governance", tags=["governance"])

# 算法名 → C++ RateLimitAlgorithm 枚举值
_ALGO_CODES = {
    "fixed_window": 0, "sliding_window_log": 1, "sliding_window_counter": 2,
    "token_bucket": 3, "leaky_bucket": 4,
}


def _sync_rules_to_redis():
    """规则变更后热下发：聚合生效规则 → Redis 哈希 + INCR 版本号。

    provider 内 GovernanceWatcher 轮询版本号秒级拉取，热更新限流参数——
    页面改规则 → 线上 RPC 立即生效（治理闭环）。
    生效规则取最新一条 enabled 规则（id 最大）；无则下发 enable=false。
    """
    with SessionLocal() as session:
        rule = (session.query(RateLimitRule)
                .filter(RateLimitRule.enabled.is_(True))
                .order_by(RateLimitRule.id.desc())
                .first())
    try:
        r = redis_lib.Redis(host=REDIS_HOST, port=REDIS_PORT,
                            decode_responses=True, socket_timeout=2)
        pipe = r.pipeline()
        if rule:
            pipe.hset("rules:provider:ratelimit", mapping={
                "max_qps": str(rule.max_qps),
                "window_sec": "1",
                "algorithm": str(_ALGO_CODES.get(rule.algorithm, 3)),
                "enable": "true",
                "service_name": rule.service_name,
                "method_name": rule.method_name,
            })
        else:
            pipe.hset("rules:provider:ratelimit", mapping={"enable": "false"})
        pipe.incr("rules:version")
        pipe.execute()
        logger.info("governance rules synced to redis: rule_id=%s",
                    rule.id if rule else None)
    except Exception as e:
        logger.warning("sync rules to redis failed: %s", e)


def _log_audit(session, action, detail=None):
    from ..models.database import AuditLog
    session.add(AuditLog(action=action, resource_type="governance", detail=detail or {}))


# ── Rate Limit Rules ─────────────────────────────────────────

@router.get("/ratelimit", response_model=List[RateLimitRuleResponse])
def list_ratelimit_rules():
    with SessionLocal() as session:
        return session.query(RateLimitRule).order_by(RateLimitRule.id.desc()).all()


@router.post("/ratelimit", response_model=RateLimitRuleResponse, status_code=201)
def create_ratelimit_rule(rule: RateLimitRuleCreate):
    with SessionLocal() as session:
        db_rule = RateLimitRule(**rule.model_dump())
        session.add(db_rule)
        _log_audit(session, "ratelimit.create", rule.model_dump())
        session.commit()
        session.refresh(db_rule)
    _sync_rules_to_redis()
    return db_rule


@router.put("/ratelimit/{rule_id}", response_model=RateLimitRuleResponse)
def update_ratelimit_rule(rule_id: int, update: RateLimitRuleUpdate):
    with SessionLocal() as session:
        rule = session.query(RateLimitRule).get(rule_id)
        if not rule:
            raise HTTPException(404, "Rule not found")
        for field, value in update.model_dump(exclude_unset=True).items():
            setattr(rule, field, value)
        _log_audit(session, "ratelimit.update", {"rule_id": rule_id, **update.model_dump(exclude_unset=True)})
        session.commit()
        session.refresh(rule)
    _sync_rules_to_redis()
    return rule


@router.delete("/ratelimit/{rule_id}")
def delete_ratelimit_rule(rule_id: int):
    with SessionLocal() as session:
        rule = session.query(RateLimitRule).get(rule_id)
        if not rule:
            raise HTTPException(404, "Rule not found")
        _log_audit(session, "ratelimit.delete", {"rule_id": rule_id})
        session.delete(rule)
        session.commit()
    _sync_rules_to_redis()
    return {"detail": "deleted"}


# ── Circuit Breaker Rules ─────────────────────────────────────

@router.get("/circuitbreaker", response_model=List[CircuitBreakerRuleResponse])
def list_breaker_rules():
    with SessionLocal() as session:
        return session.query(CircuitBreakerRule).order_by(CircuitBreakerRule.id.desc()).all()


@router.post("/circuitbreaker", response_model=CircuitBreakerRuleResponse, status_code=201)
def create_breaker_rule(rule: CircuitBreakerRuleCreate):
    with SessionLocal() as session:
        db_rule = CircuitBreakerRule(**rule.model_dump())
        session.add(db_rule)
        _log_audit(session, "circuitbreaker.create", rule.model_dump())
        session.commit()
        session.refresh(db_rule)
        return db_rule


@router.put("/circuitbreaker/{rule_id}", response_model=CircuitBreakerRuleResponse)
def update_breaker_rule(rule_id: int, update: CircuitBreakerRuleUpdate):
    with SessionLocal() as session:
        rule = session.query(CircuitBreakerRule).get(rule_id)
        if not rule:
            raise HTTPException(404, "Rule not found")
        for field, value in update.model_dump(exclude_unset=True).items():
            setattr(rule, field, value)
        _log_audit(session, "circuitbreaker.update", {"rule_id": rule_id})
        session.commit()
        session.refresh(rule)
        return rule


@router.delete("/circuitbreaker/{rule_id}")
def delete_breaker_rule(rule_id: int):
    with SessionLocal() as session:
        rule = session.query(CircuitBreakerRule).get(rule_id)
        if not rule:
            raise HTTPException(404, "Rule not found")
        _log_audit(session, "circuitbreaker.delete", {"rule_id": rule_id})
        session.delete(rule)
        session.commit()
        return {"detail": "deleted"}
