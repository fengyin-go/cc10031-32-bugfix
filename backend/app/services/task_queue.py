"""任务队列领域服务 — 消费编排结果生成执行任务。

数据工程核心：数据标准化与字段映射转换 — 跨模块数据产物

数据流:
    channel("rpa.steps_submitted") ← step_editor
    → task_queue 消费 → 生成排队任务 + execution_log 记录

执行计划约束:
    处理项只引用“已归属流程版本快照”的步骤，按稳定步骤键生成执行计划；
    未归属草稿不会出现在执行项里；同一 (流程, 版本, 步骤键) 只生成一个任务，
    重复消费返回原结果。
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.services import step_ownership
from app.services.base import BaseModuleService
from app.store import store


class TaskQueueService(BaseModuleService):
    MODULE = "task_queue"
    ENTITY = "排队任务"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["任务编号", "流程名称", "提交时间", "优先级", "预计执行", "排队时长", "当前状态"]
    REQUIRED_FIELDS = ["任务编号", "流程名称", "提交时间"]
    STATUS_ORDER = ["排队中", "执行中", "已完成", "已取消", "执行失败"]
    ACTION_RULES = {"取消任务": "已取消", "提升优先级": "排队中"}
    NEGATIVE_ACTIONS = []

    # ---- 跨模块业务方法 ----

    def build_execution_plan(self, bot_id: int,
                             version: str | None = None) -> dict[str, Any] | None:
        """按稳定步骤键为已归属步骤生成执行计划。

        数据来源是流程版本快照；没有快照（尚未发布）时返回 None，
        避免把未归属草稿当成执行项。
        """
        step_ownership.normalize_rows()
        snapshot = (
            store.flow_versions.snapshot_for_version(int(bot_id), version)
            if version else store.flow_versions.latest_snapshot(int(bot_id))
        )
        if snapshot is None:
            return None

        included: list[dict[str, Any]] = []
        dropped: list[dict[str, Any]] = []
        for index, item in enumerate(snapshot.get("steps", []), start=1):
            step = store.find("step_editor", int(item["step_id"]))
            if step is None:
                dropped.append({**item, "reason": "步骤已被删除"})
                continue
            if step_ownership.owner_of(step) != int(bot_id):
                dropped.append({**item, "reason": "步骤归属已变更"})
                continue
            if step.get("status") == "已废弃":
                dropped.append({**item, "reason": "步骤已废弃"})
                continue
            included.append({
                "seq": index,
                "step_id": int(step["id"]),
                "step_key": step_ownership.stable_step_key(step),
                "name": step.get("步骤名称"),
                "type": step.get("操作类型"),
                "element": step.get("目标元素"),
                "wait": step.get("等待策略"),
                "on_error": step.get("异常处理"),
            })

        return {
            "bot_id": int(bot_id),
            "bot_name": snapshot.get("bot_name"),
            "version": snapshot.get("version"),
            "snapshot_id": snapshot.get("snapshot_id"),
            "plan_type": "sequential",
            "total_steps": len(included),
            "steps": included,
            "dropped": dropped,
        }

    def consume_steps_to_queue(self) -> dict[str, Any]:
        """消费编排结果与流程版本快照，生成任务队列条目。

        两个入口都按稳定步骤键对齐：
        - channel("rpa.steps_submitted")：编排提交的计划；
        - channel("rpa.bot_created")：发布后的版本快照（处理项涉及的快照）。
        未归属草稿不会进入执行计划；同一 (流程, 版本, 步骤键) 只生成一个任务，
        重复消费或刷新后重新进入不会重复生成任务。

        Returns:
            {"generated": int, "plans_consumed": int, "task_ids": [...]}
        """
        step_msgs = store.channel.consume(
            consumer=self.MODULE,
            topic="rpa.steps_submitted",
        )
        version_msgs = store.channel.consume(
            consumer=self.MODULE,
            topic="rpa.bot_created",
        )
        if not step_msgs and not version_msgs:
            return {"generated": 0, "plans_consumed": 0, "task_ids": [], "reused": 0}

        task_ids: list[int] = []
        reused = 0

        for msg in step_msgs:
            created, skipped = self._enqueue_plan(msg.payload)
            task_ids.extend(created)
            reused += skipped
            store.channel.confirm(msg.msg_id, self.MODULE)

        for msg in version_msgs:
            payload = msg.payload or {}
            bot_id = int(payload.get("bot_id", 0) or 0)
            plan = self.build_execution_plan(bot_id, payload.get("version"))
            if plan is not None:
                plan["snapshot_id"] = payload.get("snapshot_id", plan.get("snapshot_id"))
                created, skipped = self._enqueue_plan(plan)
                task_ids.extend(created)
                reused += skipped
            store.channel.confirm(msg.msg_id, self.MODULE)

        generated = len(task_ids) - reused
        unique_ids = list(dict.fromkeys(task_ids))
        return {
            "generated": generated,
            "plans_consumed": len(step_msgs) + len(version_msgs),
            "task_ids": unique_ids,
            "reused": reused,
        }

    def _enqueue_plan(self, plan: dict[str, Any]) -> tuple[list[int], int]:
        """把一个执行计划写进队列，按稳定步骤键去重。"""
        task_ids: list[int] = []
        reused = 0
        bot_id = int(plan.get("bot_id", 0) or 0)
        version = plan.get("version")
        steps = plan.get("steps", [])
        if not steps:
            return task_ids, reused

        bot = store.find("bot_definition", bot_id)
        bot_name = plan.get("bot_name") or (
            bot.get("流程名称") if bot else f"Bot-{bot_id}")

        for i, step in enumerate(steps):
            seq = int(step.get("seq") or i + 1)
            step_key = str(step.get("step_key") or "")
            step_entry = store.find("step_editor", int(step.get("step_id", 0) or 0))
            if step_entry is not None:
                step_key = step_ownership.stable_step_key(step_entry)
                # 只把确实归属该流程的步骤放进执行计划
                if step_ownership.owner_of(step_entry) != bot_id:
                    continue
            if not step_key:
                step_key = f"SEQ-{seq}-S{step.get('step_id', 0)}"

            existing = self._find_task(bot_id, version, step_key)
            if existing is not None:
                task_ids.append(int(existing["id"]))
                reused += 1
                continue

            now = datetime.now(timezone.utc)
            values = {
                "任务编号": f"TASK-{bot_id}-{seq:03d}-{step_key[:12]}",
                "流程名称": bot_name,
                "提交时间": now.strftime("%Y-%m-%d %H:%M:%S"),
                "优先级": "高" if seq <= 3 else "中",
                "预计执行": step.get("name", f"Step-{seq}"),
                "排队时长": "0s",
                "当前状态": "排队中",
            }
            entry, missing = self.create_entry(values)
            if not entry:
                continue
            entry["step_id"] = step.get("step_id")
            entry["step_key"] = step_key
            entry["bot_id"] = bot_id
            entry["version"] = version
            entry["snapshot_id"] = plan.get("snapshot_id")
            entry["step_type"] = step.get("type", "")
            task_ids.append(int(entry["id"]))

        return task_ids, reused

    def _find_task(self, bot_id: int, version: str | None,
                   step_key: str) -> dict[str, Any] | None:
        """按 (流程, 版本, 稳定步骤键) 查找已存在的执行任务。"""
        for row in store.rows(self.MODULE):
            if (row.get("bot_id") == bot_id
                    and row.get("step_key") == step_key
                    and row.get("version") == version):
                return row
        return None

    def update_queue_status(self, task_id: int,
                             status: str, result: dict[str, Any] | None = None) -> dict[str, Any] | None:
        """更新任务状态并在执行完成时写 execution_log。

        Args:
            task_id: 任务 ID
            status: 目标状态（"执行中", "已完成", "执行失败"）
            result: 执行结果数据（可选）

        Returns:
            更新后的任务信息或 None
        """
        entry = store.find(self.MODULE, task_id)
        if entry is None:
            return None

        valid_statuses = {"执行中", "已完成", "已取消", "执行失败"}
        if status not in valid_statuses:
            return None

        entry["status"] = status
        entry["pending"] = status not in ("已完成", "已取消")
        entry["abnormal"] = status == "执行失败"

        if result:
            entry["result"] = result

        # 完成后写入执行日志通道
        if status in ("已完成", "执行失败"):
            exec_payload = {
                "task_id": task_id,
                "task_number": entry.get("任务编号"),
                "final_status": status,
                "processed_by": self.MODULE,
                "result": result or {},
            }
            store.channel.publish(
                topic="rpa.task_completed",
                source=self.MODULE,
                payload=exec_payload,
            )

        return {
            "task_id": task_id,
            "status": status,
            "task_number": entry.get("任务编号"),
        }

    def schedule_next_batch(self, max_tasks: int = 5) -> dict[str, Any]:
        """从排队中取下一批任务标记为执行中。

        模拟调度器的取任务逻辑 — 按优先级 + 提交时间排序。

        Returns:
            {"scheduled": int, "task_ids": [...]}
        """
        rows = store.rows(self.MODULE)
        queued = [
            r for r in rows
            if r.get("status") == "排队中" and not r.get("abnormal")
        ]
        # 按优先级排序（高 > 中 > 低）
        priority_order = {"高": 0, "中": 1, "低": 2}
        queued.sort(key=lambda r: priority_order.get(r.get("优先级", "低"), 99))

        scheduled = []
        for task in queued[:max_tasks]:
            task["status"] = "执行中"
            task["pending"] = True
            task["开始执行时间"] = datetime.now(timezone.utc).strftime(
                "%Y-%m-%d %H:%M:%S")
            scheduled.append(task["id"])
            # 模拟执行记录
            store.channel.publish(
                topic="rpa.task_started",
                source=self.MODULE,
                payload={
                    "task_id": task["id"],
                    "task_number": task.get("任务编号"),
                    "step_key": task.get("step_key"),
                    "start_time": task["开始执行时间"],
                },
            )

        return {"scheduled": len(scheduled), "task_ids": scheduled}

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        # 通道消费统计
        consumed = store.channel.snapshot(topic="rpa.steps_submitted")
        completed = store.channel.snapshot(topic="rpa.task_completed")
        rows = store.rows(self.MODULE)
        return {
            **base,
            "steps_plans_consumed": len(consumed),
            "tasks_completed": len(completed),
            "tasks_with_step_key": sum(1 for r in rows if r.get("step_key")),
        }
