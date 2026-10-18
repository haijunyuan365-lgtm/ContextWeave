"""Protected historical metadata and explicitly submitted regression fixtures."""
import asyncio
from dataclasses import asdict
import hmac
import os
import re
import sqlite3
from typing import Literal, Optional

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Request
from pydantic import BaseModel, Field
from observability.history import get_history


def authorized_history(request: Request, authorization: Optional[str] = Header(default=None)):
    token = os.getenv("TRACE_ADMIN_TOKEN", "").strip()
    account = getattr(request.state, "account", None)
    admin = bool(account and account.get("role") == "admin")
    if not token and not admin:
        raise HTTPException(503, "历史追踪接口未配置访问令牌")
    expected = ("Bearer " + token).encode("utf-8")
    if not admin and not hmac.compare_digest((authorization or "").encode("utf-8"), expected):
        raise HTTPException(401, "追踪访问令牌无效", headers={"WWW-Authenticate": "Bearer"})
    store = get_history()
    if store is None:
        raise HTTPException(503, "历史追踪服务未启用或未就绪")
    return store


def valid_trace_id(trace_id):
    if not re.fullmatch(r"[0-9a-f]{32}", trace_id) or int(trace_id, 16) == 0:
        raise HTTPException(422, "trace_id 必须是非零的 32 位小写十六进制 ID")
    return trace_id


async def history_io(function, *args, **kwargs):
    try:
        return await asyncio.to_thread(function, *args, **kwargs)
    except (sqlite3.Error, OSError):
        raise HTTPException(503, "历史追踪存储暂不可用") from None


class RegressionInput(BaseModel):
    # Operators submit reviewed test text; ordinary chat prompts are never copied here.
    turns: list[str] = Field(min_length=1, max_length=10)


def build_router(evaluator_getter):
    router = APIRouter(prefix="/trace", tags=["历史追踪"])

    @router.get("/history")
    async def search_history(request_id: Optional[str] = Query(None, max_length=64),
                             outcome: Optional[Literal["success", "error", "degraded", "skipped"]] = None,
                             since_ns: Optional[int] = Query(None, ge=0, le=(1 << 63)-1),
                             limit: int = Query(20, ge=1, le=100), offset: int = Query(0, ge=0, le=100000),
                             store=Depends(authorized_history)):
        return await history_io(store.search, request_id=request_id, outcome=outcome, since_ns=since_ns, limit=limit, offset=offset)

    @router.get("/history/{trace_id}")
    async def trace_detail(trace_id: str, store=Depends(authorized_history)):
        detail = await history_io(store.detail, valid_trace_id(trace_id))
        if detail is None:
            raise HTTPException(404, "Trace 未保存、已过期或已删除")
        return detail

    @router.delete("/history/{trace_id}")
    async def delete_trace(trace_id: str, store=Depends(authorized_history)):
        return {"deleted": await history_io(store.delete, valid_trace_id(trace_id))}

    @router.get("/regressions")
    async def regression_candidates(limit: int = Query(20, ge=1, le=100), offset: int = Query(0, ge=0, le=100000),
                                    store=Depends(authorized_history)):
        return await history_io(store.search, low_score=True, limit=limit, offset=offset)

    @router.put("/regressions/{trace_id}")
    async def save_regression(trace_id: str, body: RegressionInput, store=Depends(authorized_history)):
        if any(not turn.strip() or len(turn) > 2000 for turn in body.turns):
            raise HTTPException(422, "每轮用例需为 1 至 2000 字的非空文本")
        if not await history_io(store.save_regression, valid_trace_id(trace_id), body.turns):
            raise HTTPException(404, "找不到有效的低分 Trace 候选")
        return {"trace_id": trace_id, "replay_ready": True}

    @router.post("/regressions/{trace_id}/run")
    async def replay_regression(trace_id: str, store=Depends(authorized_history)):
        case = await history_io(store.regression, valid_trace_id(trace_id))
        if case is None:
            raise HTTPException(409, "请先提交审核后的回归用例文本")
        detail = await history_io(store.detail, trace_id)
        if detail is None:
            raise HTTPException(404, "源 Trace 已过期或已删除")
        roots = [s for s in detail["spans"] if s["name"] in {"chat.pipeline", "evaluation.dialog"} or s["parent_span_id"] is None]
        if roots:
            case["source_span_id"] = roots[0]["span_id"]
        evaluator = evaluator_getter()
        if evaluator is None:
            raise HTTPException(503, "评测服务未就绪")
        return asdict(await evaluator.run(intent_cases=[], dialog_cases=[case]))

    return router
