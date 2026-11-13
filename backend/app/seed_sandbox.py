"""依赖沙盘示例数据：调度任务、正式依赖边与待发布候选边。

沙盘数据单独存放，和正式调度 DAG 的版本号一起使用。路径票会复制这里的一份快照，
因此后续正式 DAG 即使发布新版本，历史路径票仍按当时的版本留档。
"""
from __future__ import annotations

from typing import Any

SANDBOX_ROOT_TASK = "TASK-0002"
SANDBOX_START_DATE = "2026-09-25"
SANDBOX_END_DATE = "2026-10-05"

SANDBOX_TASKS: list[dict[str, Any]] = [
    {
        "task_id": "TASK-0001",
        "task_name": "订单贴源接入",
        "layer": "ODS",
        "target_table": "ods_order",
        "owner": "贴源接入组",
        "completed_watermark": "2026-10-31",
        "failure_partitions": [],
        "failure_reason": "",
    },
    {
        "task_id": "TASK-0002",
        "task_name": "订单明细清洗",
        "layer": "DWD",
        "target_table": "dwd_order_detail",
        "owner": "补数负责人",
        "completed_watermark": "2026-09-30",
        "failure_partitions": [],
        "failure_reason": "",
    },
    {
        "task_id": "TASK-0003",
        "task_name": "订单汇总聚合",
        "layer": "DWS",
        "target_table": "dws_order_summary",
        "owner": "汇总加工组",
        "completed_watermark": "2026-09-28",
        "failure_partitions": [],
        "failure_reason": "",
    },
    {
        "task_id": "TASK-0004",
        "task_name": "订单报表输出",
        "layer": "ADS",
        "target_table": "ads_order_report",
        "owner": "应用输出组",
        "completed_watermark": "2026-09-20",
        "failure_partitions": [],
        "failure_reason": "",
    },
    {
        "task_id": "TASK-0005",
        "task_name": "商品维表加工",
        "layer": "DIM",
        "target_table": "dim_product",
        "owner": "维度建模组",
        "completed_watermark": "2026-09-27",
        "failure_partitions": ["2026-10-03"],
        "failure_reason": "质量探针 QUAL-0002 阻断，补跑需先修复维表空值",
    },
    {
        "task_id": "TASK-0006",
        "task_name": "订单质量校验",
        "layer": "DQC",
        "target_table": "dqc_order_check",
        "owner": "质量组",
        "completed_watermark": "2026-09-15",
        "failure_partitions": [],
        "failure_reason": "",
    },
]

SANDBOX_FORMAL_EDGES: list[dict[str, Any]] = [
    {
        "edge_id": "EDGE-0001",
        "upstream_task": "TASK-0001",
        "downstream_task": "TASK-0002",
        "source": "formal",
        "status": "已发布",
        "created_dag_version": "v3",
    },
    {
        "edge_id": "EDGE-0002",
        "upstream_task": "TASK-0002",
        "downstream_task": "TASK-0003",
        "source": "formal",
        "status": "已发布",
        "created_dag_version": "v3",
    },
    {
        "edge_id": "EDGE-0003",
        "upstream_task": "TASK-0003",
        "downstream_task": "TASK-0004",
        "source": "formal",
        "status": "已发布",
        "created_dag_version": "v3",
    },
]

# 候选边是补数负责人准备修改 TASK-0002 依赖时保存的草稿，尚未进入正式 DAG。
# CAND-0001 是本次想新增的直连依赖；CAND-0002 会让 0002→0003→0004→0005→0002 成环；
# CAND-0003 引用了不存在的任务 TASK-0009，用来暴露缺失依赖。
SANDBOX_CANDIDATE_EDGES: list[dict[str, Any]] = [
    {
        "edge_id": "CAND-0001",
        "upstream_task": "TASK-0005",
        "downstream_task": "TASK-0002",
        "source": "candidate",
        "status": "待发布",
        "created_dag_version": "v3",
        "remark": "补数负责人拟为订单明细清洗新增商品维表直连依赖",
    },
    {
        "edge_id": "CAND-0002",
        "upstream_task": "TASK-0004",
        "downstream_task": "TASK-0005",
        "source": "candidate",
        "status": "待发布",
        "created_dag_version": "v3",
        "remark": "报表输出反向依赖维表，需在沙盘确认是否形成环",
    },
    {
        "edge_id": "CAND-0003",
        "upstream_task": "TASK-0009",
        "downstream_task": "TASK-0002",
        "source": "candidate",
        "status": "待发布",
        "created_dag_version": "v3",
        "remark": "引用的上游任务 TASK-0009 尚未登记",
    },
]

SANDBOX_DAG_HISTORY: list[dict[str, Any]] = [
    {
        "dag_version": "v2",
        "published_at": "2026-09-10T09:00:00+08:00",
        "published_by": "调度管理员",
        "note": "上一次正式依赖版本，仅保留用于历史沙盘对照",
        "edges": [],
    },
    {
        "dag_version": "v3",
        "published_at": "2026-09-20T10:30:00+08:00",
        "published_by": "调度管理员",
        "note": "当前正式依赖版本",
        "edges": [edge["edge_id"] for edge in SANDBOX_FORMAL_EDGES],
    },
]
