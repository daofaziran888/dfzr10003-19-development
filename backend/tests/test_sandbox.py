"""依赖沙盘回归用例：路径票幂等、阻塞点、模拟批次推进、正式 DAG 隔离与版本留档。

运行：cd backend && python3 -I -m unittest discover -s tests -v
"""
from __future__ import annotations

import copy
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.services.sandbox import SandboxService  # noqa: E402
from app.store import store  # noqa: E402


class SandboxTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.svc = SandboxService()
        self.backfill_backup = copy.deepcopy(store.rows("backfill"))

    def tearDown(self) -> None:
        store._tables["backfill"] = self.backfill_backup  # noqa: SLF001 测试内还原共享内存数据

    def _generate(self, start: str = "2026-09-25", end: str = "2026-10-05"):
        ticket, message = self.svc.generate_ticket(
            root_task="TASK-0002",
            start_date=start,
            end_date=end,
            operator="补数负责人",
        )
        self.assertIsNotNone(ticket, message)
        assert ticket is not None
        return ticket

    def _resolve_recommended(self, ticket) -> None:
        for point in list(ticket["blocking_points"]):
            option = next(item for item in point["options"] if item.get("recommended"))
            resolved, message = self.svc.resolve_blocking(
                ticket["ticket_id"],
                blocking_id=point["blocking_id"],
                action=option["action"],
                target=option["target"],
                remark="沙盘处理",
                operator="补数负责人",
            )
            self.assertIsNotNone(resolved, message)

    # ---------------------------------------------------------------- 路径票

    def test_generate_is_idempotent_for_same_root_and_window(self) -> None:
        first = self._generate()
        second, message = self.svc.generate_ticket(
            root_task="TASK-0002",
            start_date="2026-09-25",
            end_date="2026-10-05",
            operator="另一位值班",
        )
        self.assertIs(second, first)
        self.assertIn("沿用同一份路径票", message)
        self.assertEqual(len(self.svc.tickets), 1)

    def test_different_window_generates_distinct_ticket(self) -> None:
        first = self._generate()
        second = self._generate(start="2026-09-20", end="2026-09-24")
        self.assertNotEqual(first["ticket_id"], second["ticket_id"])

    def test_invalid_window_is_rejected(self) -> None:
        ticket, message = self.svc.generate_ticket(
            root_task="TASK-0002",
            start_date="2026-10-05",
            end_date="2026-09-25",
            operator="补数负责人",
        )
        self.assertIsNone(ticket)
        self.assertIn("开始日期不能晚于结束日期", message)

    def test_ticket_marks_upstreams_watermarks_and_pending_partitions(self) -> None:
        ticket = self._generate()
        segments = {segment["task_id"]: segment for segment in ticket["segments"]}
        self.assertEqual(segments["TASK-0001"]["role"], "read_only_upstream")
        self.assertEqual(segments["TASK-0005"]["completed_watermark"], "2026-09-27")
        self.assertEqual(segments["TASK-0005"]["resume_from"], "2026-09-28")
        self.assertEqual(segments["TASK-0005"]["pending_partition_count"], 8)
        self.assertEqual(segments["TASK-0002"]["role"], "root")
        self.assertEqual(segments["TASK-0002"]["resume_from"], "2026-10-01")
        self.assertEqual(segments["TASK-0002"]["pending_partition_count"], 5)
        self.assertEqual(ticket["blocking_watermark"], "2026-10-03")
        self.assertEqual(ticket["blocked_task"], "TASK-0005")

    # ---------------------------------------------------------------- 阻塞点

    def test_blocking_points_cover_cycle_missing_and_completed_upstream(self) -> None:
        ticket = self._generate()
        types = {point["type"] for point in ticket["blocking_points"]}
        self.assertEqual(types, {"cycle", "missing_dependency", "completed_upstream"})
        cycle = next(point for point in ticket["blocking_points"] if point["type"] == "cycle")
        self.assertIn("TASK-0004", cycle["cycle_path"])
        self.assertTrue(all(option["action"] == "defer_edge" for option in cycle["options"]))
        completed = next(
            point for point in ticket["blocking_points"] if point["type"] == "completed_upstream"
        )
        self.assertEqual(completed["task_id"], "TASK-0001")
        self.assertEqual(completed["options"][0]["action"], "exclude_task")

    def test_generation_does_not_modify_formal_edges(self) -> None:
        before = copy.deepcopy(self.svc.formal_edges)
        ticket = self._generate()
        self.assertTrue(ticket["formal_dag_untouched"])
        self.assertEqual(self.svc.formal_edges, before)
        self.assertEqual(self.svc.dag_version, "v3")

    def test_resolving_blocking_does_not_modify_formal_edges_or_candidates(self) -> None:
        before = copy.deepcopy(self.svc.formal_edges)
        candidates_before = copy.deepcopy(self.svc.candidate_edges)
        ticket = self._generate()
        self._resolve_recommended(ticket)
        self.assertEqual(self.svc.formal_edges, before)
        self.assertEqual(self.svc.candidate_edges, candidates_before)
        self.assertEqual(ticket["status"], "可执行")
        self.assertTrue(ticket["formal_dag_untouched"])

    # ---------------------------------------------------------------- 确认执行

    def test_confirm_requires_explicit_flag(self) -> None:
        ticket = self._generate()
        result, message = self.svc.confirm_execute(
            ticket["ticket_id"], confirm=False, operator="补数负责人"
        )
        self.assertIsNone(result)
        self.assertIn("显式确认", message)
        self.assertEqual(self.svc.batches, [])

    def test_confirm_is_refused_while_blocking_points_open(self) -> None:
        ticket = self._generate()
        result, message = self.svc.confirm_execute(
            ticket["ticket_id"], confirm=True, operator="补数负责人"
        )
        self.assertIsNone(result)
        self.assertIn("仍有未处理的阻塞点", message)
        self.assertEqual(self.svc.batches, [])

    def test_confirm_creates_only_backfill_0002_batch_and_stops_on_failure(self) -> None:
        ticket = self._generate()
        self._resolve_recommended(ticket)
        result, _message = self.svc.confirm_execute(
            ticket["ticket_id"], confirm=True, operator="补数负责人"
        )
        self.assertIsNotNone(result)
        batch = self.svc.get_batch("BACKFILL-0002")
        assert batch is not None
        self.assertEqual(len(self.svc.batches), 1)
        self.assertEqual(batch["batch_no"], "BACKFILL-0002")
        self.assertTrue(batch["simulated"])
        self.assertFalse(batch["formal_dag_published"])
        self.assertEqual(batch["status"], "已暂停")
        self.assertEqual(batch["blocked_task"], "TASK-0005")
        self.assertEqual(batch["blocking_watermark"], "2026-10-03")

        failed = next(segment for segment in batch["segments"] if segment["task_id"] == "TASK-0005")
        partitions = {item["partition_date"]: item["status"] for item in failed["partitions"]}
        self.assertEqual(partitions["2026-10-02"], "成功")
        self.assertEqual(partitions["2026-10-03"], "失败")
        self.assertEqual(partitions["2026-10-04"], "未开始")
        self.assertEqual(partitions["2026-10-05"], "未开始")

        downstream = next(segment for segment in batch["segments"] if segment["task_id"] == "TASK-0002")
        self.assertEqual(downstream["status"], "未开始")
        self.assertTrue(all(item["status"] == "未开始" for item in downstream["partitions"]))
        self.assertEqual(batch["edge_order"][0]["status"], "被阻塞（上游段失败）")

    def test_repeated_confirm_reuses_batch(self) -> None:
        ticket = self._generate()
        self._resolve_recommended(ticket)
        self.svc.confirm_execute(ticket["ticket_id"], confirm=True, operator="补数负责人")
        _result, message = self.svc.confirm_execute(
            ticket["ticket_id"], confirm=True, operator="补数负责人"
        )
        self.assertIn("沿用批次", message)
        self.assertEqual(len(self.svc.batches), 1)

    def test_dispatch_and_backfill_share_ticket_and_watermark(self) -> None:
        ticket = self._generate()
        self._resolve_recommended(ticket)
        self.svc.confirm_execute(ticket["ticket_id"], confirm=True, operator="补数负责人")
        batch = self.svc.get_batch("BACKFILL-0002")
        assert batch is not None
        ticket_view = self.svc.latest_ticket()
        assert ticket_view is not None
        # 调度页与重跑页读同一份路径票与边序
        self.assertEqual(ticket_view["ticket_id"], ticket["ticket_id"])
        self.assertEqual(batch["ticket_id"], ticket["ticket_id"])
        self.assertEqual(
            [edge["edge_id"] for edge in batch["edge_order"]],
            [edge["edge_id"] for edge in ticket["edge_order"] if edge["execution_included"]],
        )
        row = next(
            item for item in store.rows("backfill") if item.get("批次编号") == "BACKFILL-0002"
        )
        self.assertEqual(row["路径票号"], ticket["ticket_id"])
        self.assertEqual(row["阻塞水位"], "2026-10-03")
        self.assertEqual(row["DAG版本"], "v3")

    def test_cycle_resolution_keeps_other_cycle_reported(self) -> None:
        ticket = self._generate()
        cycle = next(point for point in ticket["blocking_points"] if point["type"] == "cycle")
        not_recommended = next(
            option for option in cycle["options"] if not option.get("recommended")
        )
        _result, message = self.svc.resolve_blocking(
            ticket["ticket_id"],
            blocking_id=cycle["blocking_id"],
            action=not_recommended["action"],
            target=not_recommended["target"],
            remark="只暂缓一条",
            operator="补数负责人",
        )
        self.assertIsNotNone(_result, message)
        self.assertEqual(ticket["status"], "阻塞待处理")

    def test_empty_window_scope_does_not_create_batch(self) -> None:
        ticket, message = self.svc.generate_ticket(
            root_task="TASK-0001",
            start_date="2026-10-01",
            end_date="2026-10-31",
            operator="补数负责人",
        )
        self.assertIsNotNone(ticket, message)
        assert ticket is not None
        self.assertTrue(ticket["scope_empty"])
        result, confirm_message = self.svc.confirm_execute(
            ticket["ticket_id"], confirm=True, operator="补数负责人"
        )
        self.assertIsNone(result)
        self.assertIn("没有待补跑分区", confirm_message)
        self.assertEqual(self.svc.batches, [])

    def test_issue_outside_closure_does_not_block(self) -> None:
        ticket, message = self.svc.generate_ticket(
            root_task="TASK-0006",
            start_date="2026-09-25",
            end_date="2026-09-30",
            operator="补数负责人",
        )
        self.assertIsNotNone(ticket, message)
        assert ticket is not None
        self.assertEqual(ticket["blocking_points"], [])
        self.assertEqual(ticket["status"], "可执行")
        self.assertEqual(len(ticket["segments"]), 1)

    # ---------------------------------------------------------------- 正式发布与留档

    def test_publish_requires_separate_confirmation(self) -> None:
        ticket = self._generate()
        self._resolve_recommended(ticket)
        result, message = self.svc.publish_dependencies(
            confirm=False, edge_ids=None, operator="调度管理员", note=""
        )
        self.assertIsNone(result)
        self.assertIn("单独的显式确认", message)
        self.assertEqual(self.svc.dag_version, "v3")

    def test_publish_refuses_missing_and_cycle_edges(self) -> None:
        ticket = self._generate()
        self._resolve_recommended(ticket)
        result, message = self.svc.publish_dependencies(
            confirm=True, edge_ids=None, operator="调度管理员", note=""
        )
        self.assertIsNone(result)
        self.assertIn("未登记任务", message)
        self.assertEqual(self.svc.dag_version, "v3")

        result, message = self.svc.publish_dependencies(
            confirm=True, edge_ids=["CAND-0001", "CAND-0002"], operator="调度管理员", note=""
        )
        self.assertIsNone(result)
        self.assertIn("成环", message)
        self.assertEqual(self.svc.dag_version, "v3")

    def test_publish_creates_new_version_and_archives_old_tickets(self) -> None:
        ticket = self._generate()
        self._resolve_recommended(ticket)
        history_entry, message = self.svc.publish_dependencies(
            confirm=True, edge_ids=["CAND-0001"], operator="调度管理员", note="确认发布"
        )
        self.assertIsNotNone(history_entry)
        self.assertEqual(history_entry["dag_version"], "v4")
        self.assertIn("CAND-0001", history_entry["published_edges"])
        reused, _message = self.svc.generate_ticket(
            root_task="TASK-0002",
            start_date="2026-09-25",
            end_date="2026-10-05",
            operator="补数负责人",
        )
        self.assertEqual(reused["ticket_id"], ticket["ticket_id"])
        self.assertEqual(reused["dag_version"], "v3")
        new_ticket = self._generate(start="2026-09-20", end="2026-09-24")
        self.assertEqual(new_ticket["dag_version"], "v4")
        archives = {
            item["dag_version"]: len(item["tickets"])
            for item in self.svc.history()["ticket_archives"]
        }
        self.assertEqual(archives, {"v4": 1, "v3": 1})


if __name__ == "__main__":
    unittest.main()
