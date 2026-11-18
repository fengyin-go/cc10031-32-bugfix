"""RPA 流程定义领域服务 — 跨模块数据流的起点。

数据工程核心：水位追踪与增量采集调度 — 跨模块数据产物

数据流:
    bot_definition (创建流程) → channel("rpa.bot_created")
    → step_editor 消费流程信息编排步骤
    → task_queue 消费编排结果生成执行队列
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any

from app.services import step_ownership
from app.services.base import BaseModuleService
from app.store import store, FlowVersionRegistry


class BotDefinitionService(BaseModuleService):
    MODULE = "bot_definition"
    ENTITY = "RPA流程"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["流程名称", "目标系统", "触发方式", "执行环境", "超时上限", "失败策略", "版本号"]
    REQUIRED_FIELDS = ["流程名称", "目标系统", "触发方式"]
    STATUS_ORDER = ["草稿", "测试中", "已发布", "已停用"]
    ACTION_RULES = {"发布流程": "测试中", "停用流程": "已发布"}
    NEGATIVE_ACTIONS = ["停用流程"]

    # ---- 跨模块业务方法 ----

    def publish_bot(self, bot_id: int,
                    include_step_ids: list[int] | None = None) -> dict[str, Any] | None:
        """发布流程：变更状态并写入跨模块数据通道，供 step_editor 消费。

        数据流：bot_definition → channel("rpa.bot_created") → step_editor

        归属规则：只把“显式归属本流程”的步骤写入版本快照；
        未归属草稿即使名称相似也不会被挂到该流程，最多进入待认领队列。
        人工确认归属的步骤可通过 include_step_ids 一起发布，
        其中任何一个已被别的流程占用都会让整版落到 32 号处理终态。
        并发发布按发布键幂等，同一份内容只接受一个版本。

        Returns:
            成功时返回 channel 消息概要，失败返回 None
        """
        entry = store.find(self.MODULE, bot_id)
        if entry is None:
            return None
        # 状态流转：草稿/测试中首次发布；已发布可再次发布生成新版本，已停用不可发布
        current = entry.get("status")
        if current not in ("草稿", "测试中", "已发布"):
            return None

        bot_name = str(entry.get("流程名称") or f"Bot-{bot_id}")
        # 既有字段口径兼容：版本号可能是样例文案，统一收敛成 x.y.z
        base_version = FlowVersionRegistry.normalize_version(
            entry.get("版本号") or entry.get("version"))
        step_ownership.normalize_rows()

        # 1) 只取显式归属的步骤，未归属草稿不参与版本编排
        owned = step_ownership.owned_steps(int(bot_id))
        if include_step_ids:
            existing_ids = {int(step["id"]) for step in owned}
            for step_id in include_step_ids:
                step = store.find("step_editor", int(step_id))
                if step is not None and int(step["id"]) not in existing_ids:
                    owned.append(step)
                    existing_ids.add(int(step["id"]))
        for step in owned:
            step_ownership.ensure_step_key(step)

        version = store.flow_versions.next_version(int(bot_id), base_version)
        fingerprint = self._steps_fingerprint(owned)
        publish_key = FlowVersionRegistry.make_publish_key(
            int(bot_id), base_version,
            [step["step_key"] for step in owned], fingerprint)

        # 处理项：同一发布键再次进入返回原结果，不会生成第二个版本
        item = store.ledger.begin(
            publish_key,
            bot_id=int(bot_id),
            bot_name=bot_name,
            requested_version=version,
            base_version=base_version,
        )
        if item.get("result") is not None:
            committed = item["result"]
            entry["version"] = committed.get("version", version)
            return {**committed, "replayed": True}

        # 2) 原子写入版本快照（幂等 + 唯一归属）
        outcome = store.flow_versions.publish_version(
            publish_key=publish_key,
            bot_id=int(bot_id),
            bot_name=bot_name,
            version=version,
            base_version=base_version,
            steps=owned,
        )
        if outcome["status"] == "conflict":
            # 归属冲突：不发布、不挂载，流程落到 32 号冲突终态记录
            entry["编排终态"] = outcome["conflict"]["terminal_state"]
            entry["终态码"] = outcome["conflict"]["terminal_code"]
            entry["abnormal"] = True
            result = {
                "bot_id": int(bot_id),
                "status": current,
                "published": False,
                "conflict": outcome.get("conflict"),
                "terminal_code": outcome["conflict"]["terminal_code"],
                "message": outcome["message"],
            }
            store.ledger.commit(publish_key, result)
            self._sync_publish_todos(entry, bot_name, result)
            return result

        snapshot = outcome["snapshot"]
        if outcome["status"] == "replayed":
            # 并发重复请求：返回既有版本，状态与版本号保持不变
            entry["version"] = snapshot["version"]
            result = {
                "bot_id": int(bot_id),
                "status": entry.get("status"),
                "published": True,
                "replayed": True,
                "version": snapshot["version"],
                "snapshot_id": snapshot["snapshot_id"],
                "linked_steps": len(snapshot["step_ids"]),
                "msg_id": snapshot.get("msg_id"),
                "topic": "rpa.bot_created",
            }
            store.ledger.commit(publish_key, result)
            self._sync_publish_todos(entry, bot_name, result)
            return result

        # 3) 状态与版本只在快照写成功后变更
        entry["status"] = "已发布"
        entry["pending"] = False
        entry["版本号"] = snapshot["version"]
        entry["version"] = snapshot["version"]
        entry["编排终态"] = snapshot["terminal_state"]
        entry["终态码"] = snapshot["terminal_code"]
        if snapshot.get("conflict_step_ids"):
            entry["abnormal"] = True

        for step in owned:
            if step["id"] in snapshot["step_ids"]:
                # 归属与版本写回步骤行，保证三面字段口径一致
                step[step_ownership.OWNER_ID_FIELD] = int(bot_id)
                step[step_ownership.OWNER_NAME_FIELD] = bot_name
                step[step_ownership.CLAIM_STATE_FIELD] = step_ownership.CLAIM_OWNED
                step[step_ownership.VERSION_FIELD] = snapshot["version"]

        # 写入跨模块数据通道
        channel_payload = {
            "bot_id": bot_id,
            "bot_name": entry.get("流程名称"),
            "target_system": entry.get("目标系统"),
            "trigger_mode": entry.get("触发方式"),
            "exec_env": entry.get("执行环境"),
            "timeout": entry.get("超时上限", "300s"),
            "failure_strategy": entry.get("失败策略", "重试"),
            "version": entry.get("version"),
            "snapshot_id": snapshot["snapshot_id"],
            "step_ids": snapshot["step_ids"],
            "step_keys": snapshot["step_keys"],
            # 未归属草稿即使名称相似也不会进入快照
            "unassigned_excluded": len(step_ownership.unowned_steps()),
            "pending_claim": len(step_ownership.pending_claim_candidates(bot_name)),
        }
        msg = store.channel.publish(
            topic="rpa.bot_created",
            source=self.MODULE,
            payload=channel_payload,
        )
        store.flow_versions.attach_message(snapshot["snapshot_id"], msg.msg_id)
        result = {
            "bot_id": bot_id,
            "status": entry["status"],
            "msg_id": msg.msg_id,
            "topic": msg.topic,
            "published": True,
            "version": snapshot["version"],
            "snapshot_id": snapshot["snapshot_id"],
            "linked_steps": len(snapshot["step_ids"]),
            "excluded_unassigned": channel_payload["unassigned_excluded"],
        }
        store.ledger.commit(publish_key, result)
        self._sync_publish_todos(entry, bot_name, result)
        return result

    @staticmethod
    def _steps_fingerprint(steps: list[dict[str, Any]]) -> str:
        """步骤内容指纹：只有内容变化时才需要新的版本。"""
        payload = [
            {
                "key": step.get("step_key"),
                "seq": step.get("步骤序号"),
                "name": step.get("步骤名称"),
                "type": step.get("操作类型"),
                "element": step.get("目标元素"),
                "params": step.get("输入参数"),
                "wait": step.get("等待策略"),
                "on_error": step.get("异常处理"),
            }
            for step in steps
        ]
        raw = json.dumps(payload, ensure_ascii=False, sort_keys=True)
        return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:12]

    def _sync_publish_todos(self, entry: dict[str, Any], bot_name: str,
                            result: dict[str, Any]) -> None:
        """跨业务面同步待办：发布结果同时体现在流程定义、步骤编排、任务队列。"""
        bot_id = int(entry["id"])
        version = result.get("version")
        done = bool(result.get("published"))
        state = "已完成" if done else "待处理"
        detail = {
            "version": version,
            "snapshot_id": result.get("snapshot_id"),
            "linked_steps": result.get("linked_steps", 0),
            "terminal_code": result.get("terminal_code"),
        }
        store.ledger.sync_todo(
            "bot_definition", f"bot:{bot_id}", bot_id=bot_id,
            title=f"{bot_name} 版本发布", state=state, detail=detail)
        store.ledger.sync_todo(
            "step_editor", f"bot:{bot_id}", bot_id=bot_id,
            title=f"{bot_name} 步骤归属确认", state=state, detail=detail)
        store.ledger.sync_todo(
            "task_queue", f"bot:{bot_id}", bot_id=bot_id,
            title=f"{bot_name} 执行项入队", state=state, detail=detail)

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        """流程列表上的动作也走归属编排：发布必须生成版本快照。

        直接改状态会绕过步骤归属校验，因此“发布流程”统一委托给 publish_bot；
        未归属草稿不会被列表动作顺带挂到新版本上。
        """
        if action != "发布流程":
            return super().run_action(entry_id, action)

        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
        # 列表上的“发布流程”等价于把流程推进到可发布状态后再生成版本
        if entry.get("status") == "草稿":
            entry["status"] = "测试中"
        result = self.publish_bot(entry_id)
        if result is None:
            return None, f"{self.ENTITY}当前状态不可发布"
        if not result.get("published"):
            return None, result.get("message", "版本发布失败")
        return entry, f"{self.ENTITY}已发布版本 {result['version']}"

    def query_active_bots(self) -> list[dict[str, Any]]:
        """查询所有已发布（活跃）的流程，供调度和编排查询。"""
        rows = store.rows(self.MODULE)
        return [
            {
                "id": r.get("id"), "name": r.get("流程名称"),
                "target": r.get("目标系统"), "trigger": r.get("触发方式"),
                "env": r.get("执行环境"), "timeout": r.get("超时上限"),
                "version": r.get("版本号"),
            }
            for r in rows if r.get("status") == "已发布"
        ]

    def bot_summary_for_editor(self) -> list[dict[str, Any]]:
        """为 step_editor 提供流程摘要信息。

        消费方：step_editor.enrich_steps_from_channel()
        输出字段与 channel 消息格式对齐。
        """
        active = self.query_active_bots()
        return [
            {
                "bot_id": b["id"], "bot_name": b["name"],
                "target_system": b["target"], "trigger_mode": b["trigger"],
                "available_steps": self._estimate_step_count(b),
            }
            for b in active
        ]

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        active = self.query_active_bots()
        # 补充 channel 侧的流程消费统计
        bot_topics = store.channel.snapshot(topic="rpa.bot_created")
        return {
            **base,
            "active_bots": len(active),
            "channel_events": len(bot_topics),
            "latest_channel_event": bot_topics[-1] if bot_topics else None,
            "version_snapshots": len(store.flow_versions.list_snapshots()),
            "ledger": store.ledger.snapshot(),
        }

    @staticmethod
    def _estimate_step_count(bot: dict[str, Any]) -> int:
        """估算流程所需步骤数（基于目标系统复杂度）。"""
        complexity_map = {"ERP": 12, "外部数据源": 8, "CMS": 6, "交易平台": 10, "OA": 5}
        target = bot.get("target", "")
        for keyword, count in complexity_map.items():
            if keyword in target:
                return count
        return 6  # 默认最小步骤