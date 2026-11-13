"""依赖沙盘接口：路径票生成与复用、阻塞点处理、模拟批次执行、正式依赖发布与历史留档。

调度页（/etl）和历史重跑页（/backfill）都读取这里同一份路径票，避免两页各算一套边序。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException

from app.schemas import (
    DependencyPublishRequest,
    SandboxBatchResult,
    SandboxExecuteRequest,
    SandboxHistory,
    SandboxResolutionRequest,
    SandboxResult,
    SandboxTicketRequest,
)
from app.services.sandbox import sandbox_service

router = APIRouter(prefix="/api/sandbox", tags=["依赖沙盘"])


@router.get("/context", response_model=dict)
def get_context() -> dict[str, Any]:
    """沙盘上下文：当前正式 DAG 版本、示例任务、正式边与待发布候选边。"""
    return sandbox_service.context()


@router.get("/tickets", response_model=list[dict])
def list_tickets() -> list[dict[str, Any]]:
    """路径票列表：调度页和重跑页共用，状态与阻塞数量同源。"""
    return sandbox_service.list_tickets()


@router.get("/tickets/latest", response_model=dict)
def get_latest_ticket() -> dict[str, Any]:
    """最近一张路径票；还没有生成过时返回空说明而不是报错。"""
    ticket = sandbox_service.latest_ticket()
    if ticket is None:
        return {"ticket": None, "message": "尚未生成路径票"}
    return {"ticket": ticket}


@router.get("/tickets/{ticket_id}", response_model=dict)
def get_ticket(ticket_id: str) -> dict[str, Any]:
    """读取单张路径票（含当时 DAG 快照、边序与阻塞水位）。"""
    ticket = sandbox_service.get_ticket(ticket_id)
    if ticket is None:
        raise HTTPException(status_code=404, detail=f"路径票 {ticket_id} 不存在")
    return ticket


@router.post("/tickets", response_model=SandboxResult)
def generate_ticket(payload: SandboxTicketRequest) -> SandboxResult:
    """生成依赖沙盘路径票：同一根任务与日期窗重复生成会沿用同一张票。"""
    existed = any(
        item["idempotency_key"] == f"{payload.root_task}|{payload.start_date}|{payload.end_date}"
        for item in sandbox_service.list_tickets()
    )
    ticket, message = sandbox_service.generate_ticket(
        root_task=payload.root_task,
        start_date=payload.start_date,
        end_date=payload.end_date,
        operator=payload.operator or "补数负责人",
        include_candidate_edges=payload.include_candidate_edges,
    )
    if ticket is None:
        return SandboxResult(ok=False, message=message)
    return SandboxResult(ok=True, message=message, ticket=ticket, reused=existed)


@router.post("/tickets/{ticket_id}/blocking-points/{blocking_id}/resolve", response_model=SandboxResult)
def resolve_blocking_point(
    ticket_id: str,
    blocking_id: str,
    payload: SandboxResolutionRequest,
) -> SandboxResult:
    """处理阻塞点：只允许暂缓候选边或把已完成上游设为只读，正式 DAG 不变。"""
    ticket, message = sandbox_service.resolve_blocking(
        ticket_id,
        blocking_id=blocking_id,
        action=payload.action,
        target=payload.target,
        remark=payload.remark or "",
        operator=payload.operator or "补数负责人",
    )
    if ticket is None:
        return SandboxResult(ok=False, message=message)
    return SandboxResult(ok=True, message=message, ticket=ticket)


@router.post("/tickets/{ticket_id}/execute", response_model=SandboxBatchResult)
def execute_ticket(ticket_id: str, payload: SandboxExecuteRequest) -> SandboxBatchResult:
    """确认执行路径票：只创建 BACKFILL-0002 模拟批次，按边序推进，不改正式 DAG。"""
    ticket, message = sandbox_service.confirm_execute(
        ticket_id,
        confirm=payload.confirm,
        operator=payload.operator or "补数负责人",
    )
    if ticket is None:
        return SandboxBatchResult(ok=False, message=message)
    batch = sandbox_service.get_batch(ticket["execution"].get("batch_id") or "")
    return SandboxBatchResult(ok=True, message=message, ticket=ticket, batch=batch)


@router.get("/batches/{batch_no}", response_model=dict)
def get_batch(batch_no: str) -> dict[str, Any]:
    """读取模拟批次的分段推进结果；不存在时给出可读说明。"""
    batch = sandbox_service.get_batch(batch_no)
    if batch is None:
        raise HTTPException(status_code=404, detail=f"模拟批次 {batch_no} 尚未创建")
    return batch


@router.get("/history", response_model=SandboxHistory)
def get_history() -> SandboxHistory:
    """历史沙盘留档：路径票按生成时的 DAG 版本归集，后续发布不会改写旧票。"""
    return SandboxHistory(**sandbox_service.history())


@router.post("/dependencies/publish", response_model=dict)
def publish_dependencies(payload: DependencyPublishRequest) -> dict[str, Any]:
    """正式依赖发布：必须单独确认；发布生成新 DAG 版本，既有路径票按旧版本留档。"""
    version, message = sandbox_service.publish_dependencies(
        confirm=payload.confirm,
        edge_ids=payload.edge_ids,
        operator=payload.operator or "调度管理员",
        note=payload.note or "",
    )
    if version is None:
        return {"ok": False, "message": message, "dag_version": sandbox_service.dag_version}
    return {"ok": True, "message": message, "dag_version": version["dag_version"], "version": version}
