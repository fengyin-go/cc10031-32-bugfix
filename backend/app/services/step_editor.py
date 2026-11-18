"""步骤编排领域服务 — 消费流程信息并生成步骤定义。

数据工程核心：数据聚合计算与指标产出 — 跨模块数据产物

数据流:
    channel("rpa.bot_created") ← bot_definition
    → step_editor 消费 → channel("rpa.steps_submitted")
    → task_queue / execution_queue 消费

归属约束:
    未归属的草稿步骤不会因为名称相似被自动挂到流程上；
    名称相似只进入“待认领队列”，需显式人工确认后才写入归属。
"""
from __future__ import annotations

from typing import Any

from app.services import step_ownership
from app.services.base import BaseModuleService
from app.store import store, FlowVersionRegistry


class StepEditorService(BaseModuleService):
    MODULE = "step_editor"
    ENTITY = "流程步骤"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["步骤序号", "步骤名称", "操作类型", "目标元素", "输入参数", "等待策略", "异常处理"]
    REQUIRED_FIELDS = ["步骤序号", "步骤名称", "操作类型"]
    STATUS_ORDER = ["草稿", "已编排", "已验证", "已废弃"]
    ACTION_RULES = {"提交编排": "草稿", "验证步骤": "已编排", "废弃步骤": "已验证"}
    NEGATIVE_ACTIONS = []

    # ---- 跨模块业务方法 ----

    def enrich_steps_from_channel(self) -> dict[str, Any]:
        """消费 bot_definition 发布的流程信息，为已归属步骤补充上下文。

        只更新“确实归属该流程”的步骤上下文；未归属草稿不会被认领，
        名称相似的草稿只汇总为待认领候选，供人工确认。

        Returns:
            {"enriched": int, "bots_found": int, "details": [...]}
        """
        msgs = store.channel.consume(
            consumer=self.MODULE,
            topic="rpa.bot_created",
        )
        if not msgs:
            return {"enriched": 0, "bots_found": 0, "details": []}

        details = []
        enriched_count = 0
        for msg in msgs:
            bot_data = msg.payload
            bot_id = bot_data.get("bot_id")
            bot_name = bot_data.get("bot_name", "")
            # 仅消费显式归属该流程的步骤，不再兜底放行草稿
            steps = self._find_steps_by_bot(bot_name, bot_id=bot_id)
            for step in steps:
                step["target_system"] = bot_data.get("target_system")
                step["exec_env"] = bot_data.get("exec_env")
                enriched_count += 1

            candidates = step_ownership.pending_claim_candidates(bot_name)
            store.channel.confirm(msg.msg_id, self.MODULE)
            details.append({
                "bot_id": bot_id,
                "bot_name": bot_name,
                "linked_steps": len(steps),
                "pending_claim": len(candidates),
                "unassigned_ignored": max(
                    len(step_ownership.unowned_steps()) - len(candidates), 0),
            })

        return {
            "enriched": enriched_count,
            "bots_found": len(msgs),
            "details": details,
        }

    def _find_steps_by_bot(self, bot_name: str,
                           bot_id: int | None = None) -> list[dict[str, Any]]:
        """按显式归属查找步骤；未归属草稿一律不返回。"""
        step_ownership.normalize_rows()
        if bot_id is None:
            for bot in store.rows("bot_definition"):
                if bot.get("流程名称") == bot_name:
                    bot_id = int(bot["id"])
                    break
        if bot_id is None:
            return []
        return step_ownership.owned_steps(int(bot_id))

    def submit_steps_to_queue(self, bot_id: int,
                               steps_data: list[dict[str, Any]],
                               version: str | None = None) -> dict[str, Any]:
        """编排完成：显式归属步骤并提交到跨模块通道。

        每个步骤在创建时即写入归属流程，不会再出现“先建草稿、
        后续发布时被任意流程捡走”的中间态。

        Args:
            bot_id: 关联的流程 ID
            steps_data: 步骤列表 [{"seq": 1, "name": "打开页面", "type": "navigate", ...}, ...]
            version: 可选的流程版本（默认取流程当前版本）

        Returns:
            执行计划概要
        """
        if not steps_data:
            return {"submitted": 0, "plan_id": None, "error": "无步骤数据"}

        bot = store.find("bot_definition", int(bot_id))
        if bot is None:
            return {"submitted": 0, "plan_id": None, "error": f"流程 {bot_id} 不存在"}
        bot_name = str(bot.get("流程名称") or f"Bot-{bot_id}")
        # 版本口径统一：沿用流程当前版本（非法版本收敛为 x.y.z）
        version = FlowVersionRegistry.normalize_version(
            version or bot.get("版本号") or bot.get("version"))

        # 步骤排序（按 seq 字段）
        sorted_steps = sorted(steps_data, key=lambda s: int(s.get("seq", 0)))

        # 构造执行计划（执行项以稳定步骤键为锚点）
        execution_plan: dict[str, Any] = {
            "bot_id": int(bot_id),
            "bot_name": bot_name,
            "version": version,
            "plan_type": "sequential",
            "total_steps": len(sorted_steps),
            "steps": [],
        }

        submitted = 0
        for step in sorted_steps:
            values = {
                "步骤序号": str(step.get("seq", 0)),
                "步骤名称": step.get("name", f"Step-{step.get('seq')}"),
                "操作类型": step.get("type", "custom"),
                "目标元素": step.get("element", ""),
                "输入参数": step.get("params", ""),
                "等待策略": step.get("wait", "auto"),
                "异常处理": step.get("on_error", "retry"),
            }
            entry, missing = self.create_entry(values)
            if not entry:
                continue
            # 创建即归属：注册表原子认领，避免生成无主草稿
            claimed, conflicts = step_ownership.bind_steps(
                int(bot_id), bot_name, [int(entry["id"])], version)
            if conflicts or not claimed:
                continue
            entry = claimed[0]
            entry["status"] = "已编排"
            entry["pending"] = True
            step_key = step_ownership.ensure_step_key(entry)
            execution_plan["steps"].append({
                "step_id": entry["id"],
                "step_key": step_key,
                "seq": step.get("seq"),
                "name": step.get("name"),
                "type": step.get("type"),
            })
            submitted += 1

        # 发布到跨模块通道
        msg = store.channel.publish(
            topic="rpa.steps_submitted",
            source=self.MODULE,
            payload=execution_plan,
        )
        return {
            "submitted": submitted,
            "plan_id": f"PLAN-{bot_id}-{msg.msg_id}",
            "total_steps": len(sorted_steps),
            "msg_id": msg.msg_id,
            "version": version,
        }

    def claim_unowned_step(self, step_id: int, bot_id: int) -> dict[str, Any] | None:
        """人工认领待认领队列中的步骤：显式写入唯一归属。"""
        bot = store.find("bot_definition", int(bot_id))
        if bot is None:
            return None
        bot_name = str(bot.get("流程名称") or f"Bot-{bot_id}")
        entry = step_ownership.claim_step(step_id, int(bot_id), bot_name)
        if entry is None:
            return None
        record = store.flow_versions.add_claim({
            "step_id": int(step_id),
            "step_key": step_ownership.stable_step_key(entry),
            "bot_id": int(bot_id),
            "bot_name": bot_name,
            "claimed_by": "manual",
        })
        entry.setdefault("claim_history", []).append(record)
        return entry

    def extract_element_strategy(self, step_entries: list[int]) -> dict[str, Any]:
        """提取步骤中的元素定位策略，供 element_selector 消费。

        分析每个步骤使用的目标元素，汇总定位策略使用情况。

        Returns:
            {"total_elements": int, "by_type": dict, "recommendations": list}
        """
        element_types: dict[str, int] = {}
        recommendations = []

        for sid in step_entries:
            entry = store.find(self.MODULE, sid)
            if entry is None:
                continue
            op_type = entry.get("操作类型", "")
            element = entry.get("目标元素", "")

            if op_type:
                element_types[op_type] = element_types.get(op_type, 0) + 1
            if element:
                recommendations.append({
                    "step_id": sid,
                    "element": element,
                    "suggested_locator": f"css:{element}" if not element.startswith("xpath:") else element,
                })

        return {
            "total_elements": len(recommendations),
            "by_type": element_types,
            "recommendations": recommendations,
        }

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        owners = store.flow_versions.owner_map()
        channel_snaps = store.channel.snapshot(topic="rpa.steps_submitted")
        return {
            **base,
            "steps_submitted_to_channel": len(channel_snaps),
            "latest_submission": channel_snaps[-1] if channel_snaps else None,
            "owned_steps": len(owners),
            "unassigned_steps": len(step_ownership.unowned_steps()),
            "version_snapshots": len(store.flow_versions.list_snapshots()),
            "conflicts": len(store.flow_versions.list_conflicts()),
        }
