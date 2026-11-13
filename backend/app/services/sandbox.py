"""依赖沙盘业务规则：只读生成补跑路径票、标记阻塞点，并在确认后生成模拟批次。

沙盘和正式调度 DAG 严格隔离：
- 路径票保存生成当时的 DAG 版本快照，生成、处理阻塞点、确认执行都不会修改正式依赖边；
- 待发布的候选依赖边可以参与推演，发现环、缺失任务或已完成上游被卷入时，只允许暂缓候选边
  或把已完成上游改成只读引用，正式 DAG 保持不变；
- 正式依赖发布有单独的确认入口，发布后生成新的 DAG 版本，历史路径票仍按旧版本留档。
"""
from __future__ import annotations

import copy
from datetime import date, datetime, timedelta
from typing import Any

from app.seed_sandbox import (
    SANDBOX_CANDIDATE_EDGES,
    SANDBOX_DAG_HISTORY,
    SANDBOX_END_DATE,
    SANDBOX_FORMAL_EDGES,
    SANDBOX_ROOT_TASK,
    SANDBOX_START_DATE,
    SANDBOX_TASKS,
)
from app.store import store

BACKFILL_MODULE = "backfill"
SANDBOX_BATCH_NO = "BACKFILL-0002"
MAX_WINDOW_DAYS = 366
STATUS_BLOCKED = "阻塞待处理"
STATUS_READY = "可执行"
STATUS_EXECUTED = "已执行"
STATUS_PAUSED = "执行受阻"
STATUS_PENDING = "待确认"


def _now() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def _parse_date(value: str) -> date | None:
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def _date_range(start: date, end: date) -> list[str]:
    return [(start + timedelta(days=offset)).isoformat() for offset in range((end - start).days + 1)]


class SandboxService:
    def __init__(self) -> None:
        self.tasks: list[dict[str, Any]] = copy.deepcopy(SANDBOX_TASKS)
        self.formal_edges: list[dict[str, Any]] = copy.deepcopy(SANDBOX_FORMAL_EDGES)
        self.candidate_edges: list[dict[str, Any]] = copy.deepcopy(SANDBOX_CANDIDATE_EDGES)
        self.dag_version: str = "v3"
        self.dag_history: list[dict[str, Any]] = copy.deepcopy(SANDBOX_DAG_HISTORY)
        self.tickets: list[dict[str, Any]] = []
        self.batches: list[dict[str, Any]] = []
        self.edge_versions: list[dict[str, Any]] = []
        self._ticket_counter = 0
        self._blocking_counter = 0

    # ------------------------------------------------------------------ 公共入口

    def context(self) -> dict[str, Any]:
        """沙盘上下文：当前 DAG 版本、示例任务、正式边与待发布候选边。"""
        return {
            "dag_version": self.dag_version,
            "scenario": {
                "root_task": SANDBOX_ROOT_TASK,
                "start_date": SANDBOX_START_DATE,
                "end_date": SANDBOX_END_DATE,
                "batch_no": SANDBOX_BATCH_NO,
            },
            "tasks": copy.deepcopy(self.tasks),
            "formal_edges": copy.deepcopy(self.formal_edges),
            "candidate_edges": copy.deepcopy(self.candidate_edges),
            "edge_versions": copy.deepcopy(self.edge_versions),
            "latest_ticket_id": self.tickets[-1]["ticket_id"] if self.tickets else None,
        }

    def list_tickets(self) -> list[dict[str, Any]]:
        """路径票列表：含状态、DAG 版本与阻塞数量，供调度页和重跑页共用。"""
        return [
            {
                "ticket_id": ticket["ticket_id"],
                "idempotency_key": ticket["idempotency_key"],
                "root_task": ticket["root_task"],
                "date_window": copy.deepcopy(ticket["date_window"]),
                "dag_version": ticket["dag_version"],
                "status": ticket["status"],
                "generated_at": ticket["generated_at"],
                "unresolved_blocking_count": sum(
                    1 for point in ticket["blocking_points"] if not point["resolved"]
                ),
                "blocking_watermark": ticket["blocking_watermark"],
                "edge_order": [edge["edge_id"] for edge in ticket["edge_order"]],
                "batch_no": ticket["execution"]["batch_no"],
                "batch_status": ticket["execution"].get("batch_status", "未开始"),
                "current_dag_version": self.dag_version,
            }
            for ticket in self.tickets
        ]

    def get_ticket(self, ticket_id: str) -> dict[str, Any] | None:
        for ticket in self.tickets:
            if ticket["ticket_id"] == ticket_id:
                return ticket
        return None

    def latest_ticket(self) -> dict[str, Any] | None:
        return self.tickets[-1] if self.tickets else None

    def history(self) -> dict[str, Any]:
        """历史沙盘留档：按 DAG 版本归集路径票，版本变化不会改写既有路径票。"""
        grouped: dict[str, list[dict[str, Any]]] = {}
        for ticket in self.tickets:
            grouped.setdefault(ticket["dag_version"], []).append({
                "ticket_id": ticket["ticket_id"],
                "root_task": ticket["root_task"],
                "date_window": copy.deepcopy(ticket["date_window"]),
                "generated_at": ticket["generated_at"],
                "generated_by": ticket["generated_by"],
                "status": ticket["status"],
                "edge_order": [edge["edge_id"] for edge in ticket["edge_order"]],
                "blocking_watermark": ticket.get("blocking_watermark"),
            })
        return {
            "current_dag_version": self.dag_version,
            "versions": copy.deepcopy(self.dag_history),
            "ticket_archives": [
                {"dag_version": version, "tickets": tickets}
                for version, tickets in sorted(grouped.items(), reverse=True)
            ],
        }

    # ------------------------------------------------------------------ 路径票生成

    def generate_ticket(
        self,
        *,
        root_task: str,
        start_date: str,
        end_date: str,
        operator: str,
        include_candidate_edges: bool = True,
    ) -> tuple[dict[str, Any] | None, str]:
        """按根任务与日期窗生成路径票；同一根任务与日期窗重复生成返回同一张票。"""
        root_task = str(root_task or "").strip()
        start_date = str(start_date or "").strip()
        end_date = str(end_date or "").strip()
        if not root_task:
            return None, "根任务不能为空"
        task_map = {task["task_id"]: task for task in self.tasks}
        if root_task not in task_map:
            return None, f"根任务 {root_task} 未登记，无法推演补跑路径"
        start = _parse_date(start_date)
        end = _parse_date(end_date)
        if start is None or end is None:
            return None, "分区日期窗必须使用 YYYY-MM-DD 格式"
        if start > end:
            return None, "分区日期窗的开始日期不能晚于结束日期"
        if (end - start).days + 1 > MAX_WINDOW_DAYS:
            return None, f"分区日期窗最多 {MAX_WINDOW_DAYS} 天，请缩小范围"

        idempotency_key = f"{root_task}|{start_date}|{end_date}"
        existing = next(
            (ticket for ticket in self.tickets if ticket["idempotency_key"] == idempotency_key),
            None,
        )
        if existing is not None:
            existing["last_reused_at"] = _now()
            return existing, (
                f"已沿用同一份路径票 {existing['ticket_id']}（根任务 {root_task}，"
                f"日期窗 {start_date}~{end_date}），本次未重复生成"
            )

        self._ticket_counter += 1
        source_tasks = copy.deepcopy(self.tasks)
        source_edges = (
            copy.deepcopy(self.formal_edges) + copy.deepcopy(self.candidate_edges)
            if include_candidate_edges
            else copy.deepcopy(self.formal_edges)
        )
        ticket: dict[str, Any] = {
            "ticket_id": f"PATH-{self._ticket_counter:04d}",
            "idempotency_key": idempotency_key,
            "root_task": root_task,
            "date_window": {
                "start": start_date,
                "end": end_date,
                "days": (end - start).days + 1,
            },
            "dag_version": self.dag_version,
            "generated_at": _now(),
            "generated_by": operator or "值班管理员",
            "last_reused_at": None,
            "include_candidate_edges": include_candidate_edges,
            "status": STATUS_PENDING,
            "revision": 1,
            "snapshot": {
                "dag_version": self.dag_version,
                "tasks": source_tasks,
                "edges": source_edges,
            },
            "resolutions": [],
            "blocking_points": [],
            "segments": [],
            "edge_order": [],
            "rejected_edges": [],
            "blocking_watermark": None,
            "blocked_task": None,
            "scope_empty": False,
            "execution": {
                "batch_no": SANDBOX_BATCH_NO,
                "batch_id": None,
                "batch_status": "未开始",
                "started_at": None,
                "finished_at": None,
            },
            "formal_dag_untouched": True,
        }
        plan = self._compute_plan(ticket, first_generation=True)
        self._apply_plan(ticket, plan, first_generation=True)
        self.tickets.append(ticket)
        self._sync_backfill_todo(ticket)
        return ticket, "路径票已生成，请先处理阻塞点再确认执行；正式 DAG 未被修改"

    # ------------------------------------------------------------------ 阻塞点处理

    def resolve_blocking(
        self,
        ticket_id: str,
        *,
        blocking_id: str,
        action: str,
        target: str,
        remark: str,
        operator: str,
    ) -> tuple[dict[str, Any] | None, str]:
        """处理可修改的阻塞点：暂缓候选边或把已完成上游改成只读引用，不动正式 DAG。"""
        ticket = self.get_ticket(ticket_id)
        if ticket is None:
            return None, f"路径票 {ticket_id} 不存在"
        if ticket["execution"].get("batch_id"):
            return None, "该路径票已经确认执行，不能再修改阻塞点"
        point = next(
            (item for item in ticket["blocking_points"] if item["blocking_id"] == blocking_id),
            None,
        )
        if point is None:
            return None, f"阻塞点 {blocking_id} 不存在"
        if point["resolved"]:
            return ticket, f"阻塞点 {blocking_id} 已处理，无需重复操作"
        option = next(
            (item for item in point["options"] if item["action"] == action and item["target"] == target),
            None,
        )
        if option is None:
            return None, f"动作「{action}」的目标「{target}」不在该阻塞点的可修改范围内"

        if action == "defer_edge":
            edge = next(
                (item for item in ticket["snapshot"]["edges"]
                 if item["edge_id"] == target and item["source"] == "candidate"),
                None,
            )
            if edge is None:
                return None, f"候选边 {target} 不在本路径票的 DAG 快照里，无法暂缓"
            if target not in ticket["deferred_edges"]:
                ticket["deferred_edges"].append(target)
        elif action == "exclude_task":
            if target not in ticket["excluded_tasks"]:
                ticket["excluded_tasks"].append(target)
        else:
            return None, f"动作「{action}」不属于沙盘可修改范围"

        point["resolved"] = True
        point["resolution"] = {
            "action": action,
            "target": target,
            "remark": remark,
            "operator": operator,
            "resolved_at": _now(),
        }
        ticket["resolutions"].append({
            "blocking_id": blocking_id,
            "action": action,
            "target": target,
            "remark": remark,
            "operator": operator,
            "resolved_at": _now(),
        })
        ticket["revision"] = int(ticket.get("revision", 1)) + 1
        plan = self._compute_plan(ticket, first_generation=False)
        self._apply_plan(ticket, plan, first_generation=False)
        self._sync_backfill_todo(ticket)
        return ticket, f"阻塞点 {blocking_id} 已处理；正式 DAG 仍未被修改"

    # ------------------------------------------------------------------ 确认执行

    def confirm_execute(
        self,
        ticket_id: str,
        *,
        confirm: bool,
        operator: str,
    ) -> tuple[dict[str, Any] | None, str]:
        """确认执行路径票：只创建 BACKFILL-0002 模拟批次，按边序推进并在失败段停下。"""
        ticket = self.get_ticket(ticket_id)
        if ticket is None:
            return None, f"路径票 {ticket_id} 不存在"
        if not confirm:
            return None, "确认执行需要显式确认；路径票不会在未确认时创建批次"
        existing_batch = next(
            (batch for batch in self.batches if batch["ticket_id"] == ticket_id),
            None,
        )
        if existing_batch is not None:
            return ticket, f"路径票 {ticket_id} 已确认执行，沿用批次 {existing_batch['batch_no']}"
        if ticket["execution"].get("batch_id") and existing_batch is None:
            return None, "该路径票已有执行记录，不能重复创建批次"
        if not ticket["segments"] or all(
            not segment.get("in_backfill_scope") for segment in ticket["segments"]
        ):
            return None, "根任务在日期窗内没有待补跑分区，无需创建模拟批次"
        unresolved = [point for point in ticket["blocking_points"] if not point["resolved"]]
        if unresolved:
            return None, (
                "仍有未处理的阻塞点："
                + "、".join(point["blocking_id"] for point in unresolved)
                + "；请先处理后再确认执行"
            )

        other_batch = next(
            (batch for batch in self.batches if batch["batch_no"] == SANDBOX_BATCH_NO),
            None,
        )
        if other_batch is not None:
            return None, f"批次号 {SANDBOX_BATCH_NO} 已归属路径票 {other_batch['ticket_id']}"

        order = [
            segment for segment in ticket["segments"]
            if segment.get("in_backfill_scope", False)
        ]
        edge_order = [
            edge for edge in ticket["edge_order"] if edge.get("execution_included", False)
        ]
        batch: dict[str, Any] = {
            "batch_no": SANDBOX_BATCH_NO,
            "ticket_id": ticket_id,
            "root_task": ticket["root_task"],
            "date_window": copy.deepcopy(ticket["date_window"]),
            "dag_version": ticket["dag_version"],
            "status": "运行中",
            "created_at": _now(),
            "created_by": operator,
            "edge_order": copy.deepcopy(edge_order),
            "segments": [],
            "blocking_watermark": None,
            "blocked_task": None,
            "simulated": True,
            "formal_dag_published": False,
        }
        stopped = False
        for segment in order:
            segment_result: dict[str, Any] = {
                "seq": segment["seq"],
                "task_id": segment["task_id"],
                "task_name": segment["task_name"],
                "status": "未开始",
                "blocking_watermark": None,
                "partitions": [],
            }
            if stopped:
                segment_result["status"] = "未开始"
                segment_result["partitions"] = [
                    {"partition_date": partition, "status": "未开始", "message": "前序段失败，本段未开始"}
                    for partition in segment["pending_partitions"]
                ]
                batch["segments"].append(segment_result)
                continue

            failure_partitions = set(segment.get("simulated_failure_partitions") or [])
            failed_here = False
            for partition in segment["pending_partitions"]:
                if partition in failure_partitions:
                    segment_result["partitions"].append({
                        "partition_date": partition,
                        "status": "失败",
                        "message": segment.get("simulated_failure_reason") or "模拟失败",
                    })
                    segment_result["status"] = "失败"
                    segment_result["blocking_watermark"] = partition
                    batch["blocking_watermark"] = partition
                    batch["blocked_task"] = segment["task_id"]
                    failed_here = True
                    stopped = True
                    continue
                segment_result["partitions"].append({
                    "partition_date": partition,
                    "status": "成功",
                    "message": "模拟补跑通过",
                })
            if failed_here:
                # 同一段里失败分区之后的分区保持未开始
                seen_failure = False
                for item in segment_result["partitions"]:
                    if item["status"] == "失败":
                        seen_failure = True
                    elif seen_failure and item["status"] == "成功":
                        item["status"] = "未开始"
                        item["message"] = "失败分区之后保持未开始"
            if segment_result["status"] != "失败":
                segment_result["status"] = "成功"
            batch["segments"].append(segment_result)

        batch["status"] = "已暂停" if stopped else "已完成"
        batch["finished_at"] = _now()
        for edge in batch["edge_order"]:
            upstream = next(
                (item for item in batch["segments"] if item["task_id"] == edge["upstream_task"]),
                None,
            )
            downstream = next(
                (item for item in batch["segments"] if item["task_id"] == edge["downstream_task"]),
                None,
            )
            if upstream is not None and upstream["status"] == "失败":
                edge["status"] = "被阻塞（上游段失败）"
            elif downstream is None:
                edge["status"] = "未开始"
            elif downstream["status"] == "成功":
                edge["status"] = "已通过"
            elif downstream["status"] == "失败":
                edge["status"] = "被阻塞（下游段失败）"
            else:
                edge["status"] = "未开始"

        self.batches.append(batch)
        ticket["status"] = STATUS_PAUSED if stopped else STATUS_EXECUTED
        ticket["execution"].update({
            "batch_id": batch["batch_no"],
            "batch_status": batch["status"],
            "started_at": batch["created_at"],
            "finished_at": batch["finished_at"],
        })
        ticket["batch_summary"] = {
            "batch_no": batch["batch_no"],
            "status": batch["status"],
            "blocking_watermark": batch["blocking_watermark"],
            "blocked_task": batch["blocked_task"],
            "segments": [
                {
                    "seq": item["seq"],
                    "task_id": item["task_id"],
                    "status": item["status"],
                    "blocking_watermark": item["blocking_watermark"],
                }
                for item in batch["segments"]
            ],
        }
        self._sync_backfill_todo(ticket, batch=batch)
        message = (
            f"已创建模拟批次 {batch['batch_no']}：{batch['blocked_task']} 在分区 "
            f"{batch['blocking_watermark']} 失败，后续段保持未开始"
            if stopped
            else f"已创建模拟批次 {batch['batch_no']}，全部段模拟完成"
        )
        return ticket, message

    def get_batch(self, batch_no: str) -> dict[str, Any] | None:
        return next((batch for batch in self.batches if batch["batch_no"] == batch_no), None)

    # ------------------------------------------------------------------ 正式依赖发布

    def publish_dependencies(
        self,
        *,
        confirm: bool,
        edge_ids: list[str] | None,
        operator: str,
        note: str,
    ) -> tuple[dict[str, Any] | None, str]:
        """单独确认后才发布正式依赖边；发布生成新 DAG 版本，既有路径票按旧版本留档。"""
        if not confirm:
            return None, "正式依赖发布需要单独的显式确认；本次未发布任何依赖边"
        candidates = [
            edge for edge in self.candidate_edges
            if edge.get("status") == "待发布"
            and (not edge_ids or edge["edge_id"] in edge_ids)
        ]
        if not candidates:
            return None, "没有可发布的待发布候选依赖边"
        missing_edges = [
            edge["edge_id"] for edge in candidates
            if edge["upstream_task"] not in {task["task_id"] for task in self.tasks}
            or edge["downstream_task"] not in {task["task_id"] for task in self.tasks}
        ]
        if missing_edges:
            return None, (
                "候选依赖引用了未登记任务，不能发布："
                + "、".join(missing_edges)
                + "；请先在沙盘处理缺失依赖"
            )
        proposed = self.formal_edges + candidates
        cycle_edges = self._find_cycle_edges(proposed)
        if cycle_edges:
            return None, (
                "发布后正式 DAG 仍会成环，不能发布："
                + "、".join(edge_id for edge_id, _ in cycle_edges)
                + "；请先在沙盘暂缓相关候选边"
            )
        previous_version = self.dag_version
        self.dag_version = f"v{int(self.dag_version.lstrip('v')) + 1}"
        self.edge_versions.append({
            "dag_version": previous_version,
            "recorded_at": _now(),
            "recorded_reason": "正式依赖发布前快照",
            "edges": [
                {
                    "edge_id": edge["edge_id"],
                    "upstream_task": edge["upstream_task"],
                    "downstream_task": edge["downstream_task"],
                    "status": edge.get("status"),
                }
                for edge in self.formal_edges
            ],
        })
        published: list[dict[str, Any]] = []
        for edge in candidates:
            edge["status"] = "已发布"
            edge["created_dag_version"] = self.dag_version
            edge["published_at"] = _now()
            edge["published_by"] = operator
            self.formal_edges.append(edge)
            published.append(copy.deepcopy(edge))
        self.candidate_edges = [
            edge for edge in self.candidate_edges
            if edge["edge_id"] not in {item["edge_id"] for item in candidates}
        ]
        history_entry = {
            "dag_version": self.dag_version,
            "published_at": _now(),
            "published_by": operator,
            "note": note or "补数负责人确认发布候选依赖边",
            "edges": [edge["edge_id"] for edge in self.formal_edges],
            "published_edges": [edge["edge_id"] for edge in published],
            "previous_version": self.dag_history[-1]["dag_version"] if self.dag_history else None,
        }
        self.dag_history.append(history_entry)
        return history_entry, (
            f"正式依赖已发布为 {self.dag_version}，共 {len(published)} 条边；"
            "既有路径票仍按生成时的版本留档"
        )

    # ------------------------------------------------------------------ 计划计算

    def _compute_plan(self, ticket: dict[str, Any], *, first_generation: bool) -> dict[str, Any]:
        snapshot = ticket["snapshot"]
        tasks: dict[str, dict[str, Any]] = {
            task["task_id"]: task for task in snapshot["tasks"]
        }
        edges = copy.deepcopy(snapshot["edges"])
        deferred = set(ticket.get("deferred_edges", []))
        excluded_tasks = set(ticket.get("excluded_tasks", []))
        window = ticket["date_window"]
        start = _parse_date(window["start"])
        end = _parse_date(window["end"])
        assert start is not None and end is not None
        all_dates = _date_range(start, end)

        issues: list[dict[str, Any]] = []
        rejected_edges: list[dict[str, Any]] = []
        active_edges: list[dict[str, Any]] = []
        for edge in edges:
            if edge["edge_id"] in deferred:
                rejected_edges.append({**edge, "reject_reason": "沙盘已暂缓候选边，正式 DAG 未改动"})
                continue
            if edge["upstream_task"] not in tasks or edge["downstream_task"] not in tasks:
                missing_task = (
                    edge["upstream_task"]
                    if edge["upstream_task"] not in tasks
                    else edge["downstream_task"]
                )
                issues.append({
                    "type": "missing_dependency",
                    "key": f"missing:{edge['edge_id']}",
                    "title": "候选依赖引用了未登记任务",
                    "description": (
                        f"候选边 {edge['edge_id']}（{edge['upstream_task']} → "
                        f"{edge['downstream_task']}）引用了未登记任务 {missing_task}，"
                        "无法确定上游水位与补跑分区。"
                    ),
                    "edge_id": edge["edge_id"],
                    "task_id": missing_task,
                    "cycle_path": None,
                    "options": [{
                        "action": "defer_edge",
                        "target": edge["edge_id"],
                        "label": f"暂缓候选边 {edge['edge_id']}（正式 DAG 不变）",
                        "recommended": True,
                    }],
                })
                rejected_edges.append({**edge, "reject_reason": f"上游任务 {missing_task} 缺失"})
                continue
            active_edges.append(edge)

        cycle_edges: set[str] = set()
        while True:
            found = self._find_cycle_edges(active_edges)
            if not found:
                break
            cycle_edge_ids = {edge_id for edge_id, _ in found}
            cycle_edges.update(cycle_edge_ids)
            candidate_cycle = [
                edge for edge in active_edges
                if edge["edge_id"] in cycle_edge_ids and edge["source"] == "candidate"
            ]
            if not candidate_cycle:
                # 正式 DAG 自身成环属于既有问题，先给出阻塞点但不改正式边
                cycle_path = next(path for _, path in found)
                issues.append({
                    "type": "cycle",
                    "key": "cycle:formal",
                    "title": "正式依赖存在环",
                    "description": "正式 DAG 自身存在环：" + " → ".join(cycle_path),
                    "edge_ids": sorted(cycle_edge_ids),
                    "cycle_path": cycle_path,
                    "options": [],
                })
                break
            break_edge = max(candidate_cycle, key=lambda item: item["edge_id"])
            cycle_path = next(path for edge_id, path in found if edge_id == break_edge["edge_id"])
            issues.append({
                "type": "cycle",
                "key": f"cycle:{break_edge['edge_id']}:{'-'.join(sorted(cycle_edge_ids))}",
                "title": "候选依赖可能成环",
                "description": (
                    f"候选边 {break_edge['edge_id']}（{break_edge['upstream_task']} → "
                    f"{break_edge['downstream_task']}）加入后会形成环："
                    + " → ".join(cycle_path)
                    + "。环上的候选边不能直接进入补跑链。"
                ),
                "edge_id": break_edge["edge_id"],
                "cycle_path": cycle_path,
                "edge_ids": sorted(cycle_edge_ids),
                "options": [
                    {
                        "action": "defer_edge",
                        "target": item["edge_id"],
                        "label": f"暂缓候选边 {item['edge_id']}（{item['upstream_task']} → {item['downstream_task']}）",
                        "recommended": item["edge_id"] == break_edge["edge_id"],
                    }
                    for item in sorted(
                        candidate_cycle,
                        key=lambda item: item["edge_id"] != break_edge["edge_id"],
                    )
                ],
            })
            active_edges = [edge for edge in active_edges if edge["edge_id"] != break_edge["edge_id"]]
            rejected_edges.append({
                **break_edge,
                "reject_reason": "该候选边会让补跑链成环，暂不进入推演路径",
            })

        # 上游闭包与拓扑顺序：根任务排在最后，上游先执行
        upstream_map: dict[str, list[str]] = {}
        for edge in active_edges:
            upstream_map.setdefault(edge["downstream_task"], []).append(edge["upstream_task"])
        closure: set[str] = set()

        def walk(task_id: str) -> None:
            if task_id in closure:
                return
            closure.add(task_id)
            for upstream in upstream_map.get(task_id, []):
                walk(upstream)

        walk(ticket["root_task"])

        ordered: list[str] = []
        visiting: set[str] = set()

        def visit(task_id: str) -> None:
            if task_id in ordered or task_id in visiting:
                return
            visiting.add(task_id)
            for upstream in upstream_map.get(task_id, []):
                if upstream in closure:
                    visit(upstream)
            visiting.discard(task_id)
            ordered.append(task_id)

        visit(ticket["root_task"])

        # 与本次补跑闭包无关的候选边问题不阻塞这张路径票
        relevant_issues: list[dict[str, Any]] = []
        for issue in issues:
            if issue["type"] == "cycle":
                nodes = issue.get("cycle_path") or []
            else:
                edge = next(
                    (item for item in edges if item["edge_id"] == issue.get("edge_id")),
                    None,
                )
                nodes = [edge["upstream_task"], edge["downstream_task"]] if edge else []
            if any(node in closure for node in nodes):
                relevant_issues.append(issue)
        issues = relevant_issues

        # 已完成上游被卷入：水位已经覆盖整个日期窗，不该再补跑
        pending_by_task: dict[str, list[str]] = {}
        for task_id in ordered:
            task = tasks[task_id]
            watermark = task.get("completed_watermark") or ""
            pending_by_task[task_id] = [item for item in all_dates if item > watermark]
        for task_id in ordered:
            if task_id == ticket["root_task"]:
                continue
            if not pending_by_task[task_id] and task_id not in excluded_tasks:
                task = tasks[task_id]
                issues.append({
                    "type": "completed_upstream",
                    "key": f"completed:{task_id}",
                    "title": "已完成上游被卷入补跑链",
                    "description": (
                        f"上游任务 {task_id} {task['task_name']} 已完成水位 "
                        f"{task['completed_watermark']} 已覆盖整个日期窗 "
                        f"{window['start']}~{window['end']}，无需补跑；"
                        "若直接确认会把已完成分区重新卷入。"
                    ),
                    "task_id": task_id,
                    "edge_id": None,
                    "cycle_path": None,
                    "options": [{
                        "action": "exclude_task",
                        "target": task_id,
                        "label": f"把 {task_id} 设为只读上游（不参与补跑，正式 DAG 保留依赖）",
                        "recommended": True,
                    }],
                })

        # 逐段标记上游任务、已完成水位、待补跑分区与可能成环的边
        segments: list[dict[str, Any]] = []
        for seq, task_id in enumerate(ordered, start=1):
            task = tasks[task_id]
            task_edges = [edge for edge in edges if edge["downstream_task"] == task_id]
            incoming_edges = []
            for edge in task_edges:
                cycle_risk = (
                    edge["edge_id"] in cycle_edges
                    and edge["upstream_task"] in closure
                )
                missing = (
                    edge["upstream_task"] not in tasks
                    or edge["downstream_task"] not in tasks
                )
                incoming_edges.append({
                    "edge_id": edge["edge_id"],
                    "upstream_task": edge["upstream_task"],
                    "source": edge["source"],
                    "status": edge.get("status"),
                    "deferred": edge["edge_id"] in deferred,
                    "cycle_risk": cycle_risk,
                    "missing_dependency": missing,
                })
            pending = pending_by_task[task_id]
            failure_partitions = [
                item for item in task.get("failure_partitions", []) if item in pending
            ]
            failure_partitions = list(dict.fromkeys(failure_partitions))
            in_scope = bool(pending) and task_id not in excluded_tasks
            if task_id == ticket["root_task"]:
                role = "root"
            elif not in_scope:
                role = "read_only_upstream"
            else:
                role = "upstream"
            blocking_watermark = failure_partitions[0] if failure_partitions else None
            segments.append({
                "seq": seq,
                "task_id": task_id,
                "task_name": task["task_name"],
                "layer": task["layer"],
                "target_table": task["target_table"],
                "owner": task["owner"],
                "role": role,
                "in_backfill_scope": in_scope,
                "completed_watermark": task.get("completed_watermark"),
                "resume_from": pending[0] if pending else None,
                "blocking_watermark": blocking_watermark,
                "pending_partitions": pending,
                "pending_partition_count": len(pending),
                "simulated_failure_partitions": failure_partitions,
                "simulated_failure_reason": task.get("failure_reason") or "",
                "incoming_edges": incoming_edges,
            })

        # 边序：按下游段的顺序排列，只读上游边标记为不参与执行
        edge_order: list[dict[str, Any]] = []
        for edge in active_edges:
            if edge["downstream_task"] not in closure:
                continue
            downstream_segment = next(
                (item for item in segments if item["task_id"] == edge["downstream_task"]),
                None,
            )
            upstream_segment = next(
                (item for item in segments if item["task_id"] == edge["upstream_task"]),
                None,
            )
            if downstream_segment is None or upstream_segment is None:
                continue
            execution_included = bool(
                downstream_segment["in_backfill_scope"]
                and upstream_segment["in_backfill_scope"]
            )
            edge_order.append({
                "order": len(edge_order) + 1,
                "edge_id": edge["edge_id"],
                "upstream_task": edge["upstream_task"],
                "downstream_task": edge["downstream_task"],
                "source": edge["source"],
                "status": edge.get("status"),
                "execution_included": execution_included,
                "cycle_risk": edge["edge_id"] in cycle_edges,
            })
        edge_order.sort(key=lambda item: (
            next(
                (segment["seq"] for segment in segments if segment["task_id"] == item["downstream_task"]),
                10_000,
            ),
            item["edge_id"],
        ))
        for index, item in enumerate(edge_order, start=1):
            item["order"] = index

        scope_empty = not any(segment["in_backfill_scope"] for segment in segments)
        first_failure_segment = next(
            (
                segment for segment in segments
                if segment["in_backfill_scope"] and segment["simulated_failure_partitions"]
            ),
            None,
        )
        blocking_watermark = (
            first_failure_segment["simulated_failure_partitions"][0]
            if first_failure_segment
            else None
        )
        blocked_task = first_failure_segment["task_id"] if first_failure_segment else None

        return {
            "issues": issues,
            "segments": segments,
            "edge_order": edge_order,
            "rejected_edges": rejected_edges,
            "blocking_watermark": blocking_watermark,
            "blocked_task": blocked_task,
            "scope_empty": scope_empty,
        }

    def _apply_plan(
        self,
        ticket: dict[str, Any],
        plan: dict[str, Any],
        *,
        first_generation: bool,
    ) -> None:
        ticket["segments"] = plan["segments"]
        ticket["edge_order"] = plan["edge_order"]
        ticket["rejected_edges"] = plan["rejected_edges"]
        ticket["blocking_watermark"] = plan["blocking_watermark"]
        ticket["blocked_task"] = plan["blocked_task"]
        ticket["scope_empty"] = plan["scope_empty"]
        if first_generation:
            ticket["deferred_edges"] = []
            ticket["excluded_tasks"] = []
            ticket["blocking_points"] = [
                self._new_blocking_point(issue) for issue in plan["issues"]
            ]
        else:
            existing_keys = {point["key"] for point in ticket["blocking_points"]}
            for issue in plan["issues"]:
                if issue["key"] not in existing_keys:
                    ticket["blocking_points"].append(self._new_blocking_point(issue))
        unresolved = [point for point in ticket["blocking_points"] if not point["resolved"]]
        if unresolved:
            ticket["status"] = STATUS_BLOCKED
        elif ticket["execution"].get("batch_id"):
            ticket["status"] = STATUS_EXECUTED
        else:
            ticket["status"] = STATUS_READY
        ticket["formal_dag_untouched"] = True

    def _new_blocking_point(self, issue: dict[str, Any]) -> dict[str, Any]:
        self._blocking_counter += 1
        return {
            "blocking_id": f"BLOCK-{self._blocking_counter:04d}",
            "key": issue["key"],
            "type": issue["type"],
            "severity": "blocking",
            "title": issue["title"],
            "description": issue["description"],
            "edge_id": issue.get("edge_id"),
            "edge_ids": issue.get("edge_ids", []),
            "task_id": issue.get("task_id"),
            "cycle_path": issue.get("cycle_path"),
            "options": issue.get("options", []),
            "resolved": False,
            "resolution": None,
            "affects_formal_dag": False,
        }

    @staticmethod
    def _find_cycle_edges(
        edges: list[dict[str, Any]],
    ) -> list[tuple[str, list[str]]]:
        """返回所有位于环上的边及其环路径（上游任务 → ... → 下游任务）。"""
        adjacency: dict[str, list[tuple[str, str]]] = {}
        for edge in edges:
            adjacency.setdefault(edge["upstream_task"], []).append(
                (edge["downstream_task"], edge["edge_id"])
            )

        def path_between(source: str, target: str) -> list[str] | None:
            stack: list[tuple[str, list[str]]] = [(source, [source])]
            visited: set[str] = set()
            while stack:
                node, path = stack.pop()
                if node == target:
                    return path
                if node in visited:
                    continue
                visited.add(node)
                for next_node, _edge_id in adjacency.get(node, []):
                    if next_node not in visited:
                        stack.append((next_node, path + [next_node]))
            return None

        result: list[tuple[str, list[str]]] = []
        for edge in edges:
            if path_between(edge["downstream_task"], edge["upstream_task"]) is None:
                continue
            cycle_path = path_between(edge["downstream_task"], edge["upstream_task"])
            assert cycle_path is not None
            result.append((
                edge["edge_id"],
                [edge["upstream_task"]] + cycle_path,
            ))
        return result

    # ------------------------------------------------------------------ 重跑待办同步

    def _sync_backfill_todo(
        self,
        ticket: dict[str, Any],
        *,
        batch: dict[str, Any] | None = None,
    ) -> None:
        """把调度侧路径票状态同步到历史重跑模块的待办行，两页读到同一份路径票。"""
        rows = store.rows(BACKFILL_MODULE)
        row = next(
            (item for item in rows if item.get("批次编号") == ticket["execution"]["batch_no"]),
            None,
        )
        segments = ticket["segments"]
        scoped = [segment for segment in segments if segment["in_backfill_scope"]]
        if batch is None:
            completed_count = 0
            pending_count = len(scoped)
            status = "草稿"
            pending_flag = True
            abnormal = bool([point for point in ticket["blocking_points"] if not point["resolved"]])
            rerun_status = (
                "补跑待办（阻塞待处理）" if abnormal else "补跑待办（可确认执行）"
            )
            abnormal_partition = ticket["blocking_watermark"] or "—"
        else:
            completed_count = sum(1 for segment in batch["segments"] if segment["status"] == "成功")
            pending_count = sum(
                1 for segment in batch["segments"] if segment["status"] == "未开始"
            )
            status = "已暂停" if batch["status"] == "已暂停" else "已完成"
            pending_flag = status != "已完成"
            abnormal = batch["status"] == "已暂停"
            rerun_status = (
                f"失败段停在 {batch['blocked_task']} / {batch['blocking_watermark']}"
                if batch["status"] == "已暂停"
                else "模拟补跑完成"
            )
            abnormal_partition = batch["blocking_watermark"] or "—"
        root_segment = next(
            (segment for segment in segments if segment["task_id"] == ticket["root_task"]),
            None,
        )
        payload = {
            "批次编号": ticket["execution"]["batch_no"],
            "目标表名": root_segment["target_table"] if root_segment else ticket["root_task"],
            "开始日期": ticket["date_window"]["start"],
            "结束日期": ticket["date_window"]["end"],
            "并行数量": 1,
            "覆盖模式": "依赖沙盘分段补跑",
            "重跑状态": rerun_status,
            "异常区间": abnormal_partition,
            "路径票号": ticket["ticket_id"],
            "根任务": ticket["root_task"],
            "DAG版本": ticket["dag_version"],
            "已完成段数": completed_count,
            "未开始段数": pending_count,
            "阻塞水位": ticket["blocking_watermark"] or "—",
        }
        if row is None:
            row = {
                "id": max((int(item.get("id", 0)) for item in rows), default=0) + 1,
                "status": status,
                "pending": pending_flag,
                "abnormal": abnormal,
            }
            row.update(payload)
            rows.append(row)
        else:
            row.update(payload)
            row["status"] = status
            row["pending"] = pending_flag
            row["abnormal"] = abnormal

sandbox_service = SandboxService()
