"""步骤归属与流程版本编排回归测试。

覆盖：
1. 发布流程不会把未归属草稿挂到新版本上（原缺陷回归）
2. 名称相似的草稿只进入待认领队列，不自动归属
3. 同一步骤不能同时属于多个流程
4. 任务队列按稳定步骤键生成执行计划，重复消费幂等
5. 并发重复写入只接受一个版本；再次返回保留原结果
6. 冲突落到 32 号处理终态
7. 既有字段口径（样例文案版本号）兼容
8. 业务清单与结果台账一致、刷新重新进入结果不变
"""
from __future__ import annotations

import os
import sys
import threading

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.services import step_ownership
from app.services.bot_definition import BotDefinitionService
from app.services.step_editor import StepEditorService
from app.services.task_queue import TaskQueueService
from app.store import FlowVersionRegistry, store


def _new_bot(name: str, version: str = "1.0.0") -> int:
    service = BotDefinitionService()
    entry, missing = service.create_entry({
        "流程名称": name,
        "目标系统": "外部数据源",
        "触发方式": "手动",
        "执行环境": "默认环境",
        "超时上限": "300s",
        "失败策略": "重试",
        "版本号": version,
    })
    assert entry is not None and not missing
    return int(entry["id"])


def _new_draft_step(name: str, seq: str = "1") -> int:
    service = StepEditorService()
    entry, missing = service.create_entry({
        "步骤序号": seq,
        "步骤名称": name,
        "操作类型": "click",
        "目标元素": "#x",
        "输入参数": "",
        "等待策略": "visible",
        "异常处理": "retry",
    })
    assert entry is not None and not missing
    assert step_ownership.owner_of(entry) is None
    return int(entry["id"])


class TestPublishExcludesUnassignedDrafts:
    def test_publish_does_not_link_unowned_drafts(self) -> None:
        bot_id = _new_bot("发布隔离流程A")
        draft_ids = [_new_draft_step("发布隔离流程A 草稿一"),
                     _new_draft_step("发布隔离流程A 草稿二")]

        result = BotDefinitionService().publish_bot(bot_id)
        assert result is not None and result["published"] is True
        assert result["linked_steps"] == 0
        assert result["excluded_unassigned"] >= len(draft_ids)

        snapshot = store.flow_versions.latest_snapshot(bot_id)
        assert snapshot is not None and snapshot["step_ids"] == []
        for step_id in draft_ids:
            assert step_ownership.owner_of(store.find("step_editor", step_id)) is None

    def test_similar_name_draft_stays_unowned(self) -> None:
        bot_id = _new_bot("相似名流程B")
        draft_id = _new_draft_step("相似名流程B 待整理步骤")

        BotDefinitionService().publish_bot(bot_id)
        step = store.find("step_editor", draft_id)
        assert step_ownership.owner_of(step) is None
        assert draft_id not in store.flow_versions.latest_snapshot(bot_id)["step_ids"]

        candidates = step_ownership.pending_claim_candidates("相似名流程B")
        assert any(item["step_id"] == draft_id for item in candidates)

        # 人工认领后归属唯一且能进入下一次快照
        claimed = StepEditorService().claim_unowned_step(draft_id, bot_id)
        assert claimed is not None
        assert step_ownership.owner_of(claimed) == bot_id


class TestExclusiveOwnership:
    def test_one_step_cannot_belong_to_two_flows(self) -> None:
        bot_a = _new_bot("归属互斥流程A")
        bot_b = _new_bot("归属互斥流程B")
        step_id = _new_draft_step("归属互斥步骤")

        assert StepEditorService().claim_unowned_step(step_id, bot_a) is not None
        # 第二个流程认领同一步骤必须失败，归属保持 A
        assert StepEditorService().claim_unowned_step(step_id, bot_b) is None
        assert step_ownership.owner_of(store.find("step_editor", step_id)) == bot_a

        # 共享步骤键的两个版本快照不会出现同一步骤
        for bot_id in (bot_a, bot_b):
            BotDefinitionService().publish_bot(bot_id)
        seen: dict[int, set[int]] = {}
        for snapshot in store.flow_versions.list_snapshots():
            for sid in snapshot["step_ids"]:
                seen.setdefault(int(sid), set()).add(int(snapshot["bot_id"]))
        assert all(len(owners) == 1 for owners in seen.values())

    def test_publish_conflict_marks_terminal_code_32(self) -> None:
        registry = FlowVersionRegistry()
        step = {"id": 101, "step_key": "SEQ-1-S101-冲突步骤", "步骤序号": "1"}
        first = registry.publish_version(
            publish_key="k-a", bot_id=1, bot_name="流程A", version="1.0.0",
            base_version="1.0.0", steps=[step])
        assert first["status"] == "created"

        second = registry.publish_version(
            publish_key="k-b", bot_id=2, bot_name="流程B", version="1.0.0",
            base_version="1.0.0", steps=[step])
        assert second["status"] == "conflict"
        assert second["conflict"]["terminal_code"] == 32
        assert second["snapshot"] is None
        assert registry.owner_of(101) == 1
        assert len(registry.list_snapshots()) == 1


class TestQueueExecutionPlan:
    def test_plan_uses_stable_step_keys(self) -> None:
        bot_id = _new_bot("执行计划流程")
        StepEditorService().submit_steps_to_queue(bot_id, [
            {"seq": 1, "name": "打开页面", "type": "navigate", "element": "#open"},
            {"seq": 2, "name": "提取表格", "type": "extract", "element": ".table"},
        ])
        BotDefinitionService().publish_bot(bot_id)

        plan = TaskQueueService().build_execution_plan(bot_id)
        assert plan is not None
        assert plan["total_steps"] == 2
        keys = [step["step_key"] for step in plan["steps"]]
        assert len(keys) == len(set(keys))
        assert all(step["name"] for step in plan["steps"])

        # 重新进入后执行计划不变
        again = TaskQueueService().build_execution_plan(bot_id)
        assert [s["step_key"] for s in again["steps"]] == keys

    def test_queue_does_not_include_unowned_drafts(self) -> None:
        bot_id = _new_bot("执行计划隔离流程")
        _new_draft_step("执行计划隔离流程 草稿")
        BotDefinitionService().publish_bot(bot_id)

        plan = TaskQueueService().build_execution_plan(bot_id)
        assert plan is not None and plan["total_steps"] == 0

        before = sum(
            1 for row in store.rows("task_queue")
            if row.get("bot_id") == bot_id
        )
        TaskQueueService().consume_steps_to_queue()
        after = sum(
            1 for row in store.rows("task_queue")
            if row.get("bot_id") == bot_id
        )
        assert after == before


class TestConcurrentVersionWrite:
    def test_concurrent_publish_accepts_single_version(self) -> None:
        registry = FlowVersionRegistry()
        entered = threading.Barrier(2)
        results: list[dict] = []

        def worker(version: str) -> None:
            entered.wait(timeout=5)
            results.append(registry.publish_version(
                publish_key="same-key",
                bot_id=9,
                bot_name="并发流程",
                version=version,
                base_version="1.0.0",
                steps=[{"id": 900, "step_key": "SEQ-1-S900-并发"}],
            ))

        threads = [threading.Thread(target=worker, args=(v,)) for v in ("1.0.1", "1.0.2")]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=5)

        statuses = sorted(result["status"] for result in results)
        assert statuses == ["created", "replayed"]
        assert len(registry.list_snapshots(9)) == 1

    def test_republish_returns_original_result(self) -> None:
        bot_id = _new_bot("重复进入流程")
        StepEditorService().submit_steps_to_queue(bot_id, [
            {"seq": 1, "name": "重复进入步骤", "type": "click"},
        ])
        service = BotDefinitionService()
        first = service.publish_bot(bot_id)
        assert first is not None and first["published"]

        # 模拟再次进入并重复提交：版本与快照保持首次结果
        store.find("bot_definition", bot_id)["status"] = "测试中"
        second = service.publish_bot(bot_id)
        assert second is not None
        assert second.get("replayed") is True
        assert second["version"] == first["version"]
        assert second["snapshot_id"] == first["snapshot_id"]

        item = store.ledger.get(store.flow_versions.list_snapshots(bot_id)[0]["publish_key"])
        assert item is not None and item["result"]["version"] == first["version"]


class TestLegacyFields:
    def test_sample_version_text_is_normalized(self) -> None:
        bot_id = _new_bot("口径兼容流程", version="流程定义样例文案")
        result = BotDefinitionService().publish_bot(bot_id)
        assert result is not None
        assert result["version"] == "1.0.0"
        assert store.find("bot_definition", bot_id)["版本号"] == "1.0.0"


class TestBoardPersistenceAndLedger:
    def test_board_is_stable_across_reentry(self) -> None:
        bot_id = _new_bot("编排页稳定流程")
        StepEditorService().submit_steps_to_queue(bot_id, [
            {"seq": 1, "name": "稳定步骤一", "type": "click"},
        ])
        BotDefinitionService().publish_bot(bot_id)

        first = step_ownership.board()
        second = step_ownership.board()
        assert first["summary"] == second["summary"]
        assert first["snapshots"] == second["snapshots"]

        groups = {group["bot_id"]: group for group in second["groups"]}
        owned_keys = [s["step_key"] for s in groups[bot_id]["owned_steps"]]
        snapshot_keys = groups[bot_id]["snapshot"]["step_keys"]
        assert owned_keys == snapshot_keys

    def test_business_list_and_ledger_agree(self) -> None:
        bot_id = _new_bot("台账一致流程")
        StepEditorService().submit_steps_to_queue(bot_id, [
            {"seq": 1, "name": "台账步骤", "type": "click"},
        ])
        result = BotDefinitionService().publish_bot(bot_id)
        assert result is not None and result["published"]

        row = store.find("bot_definition", bot_id)
        item = store.ledger.get(
            store.flow_versions.list_snapshots(bot_id)[0]["publish_key"])
        assert item is not None
        assert row["status"] == "已发布"
        assert row["版本号"] == item["result"]["version"] == result["version"]
        assert store.ledger.snapshot()["todos_pending"] == 0

        surfaces = {todo["surface"] for todo in store.ledger.todos()}
        assert {"bot_definition", "step_editor", "task_queue"} <= surfaces
