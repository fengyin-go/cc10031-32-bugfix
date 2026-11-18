"""步骤归属规则 — 稳定步骤键、唯一归属与待认领候选。

关键约束（修复归属污染）：
- 步骤只能通过显式归属（bot_id / 所属流程）进入某个流程版本；
- 未归属的草稿步骤绝不因为“名称相似”被自动挂到流程上，
  名称相似只产生待认领候选，等待人工确认；
- 一个步骤一旦被某个流程认领，别的流程无法再写入（注册表原子认领）；
- 稳定步骤键在重新进入页面、重复发布后保持不变，执行计划按它对齐。
"""
from __future__ import annotations

import re
from typing import Any

from app.store import store

#: 步骤行上的显式归属字段（兼容既有字段口径）
OWNER_ID_FIELD = "bot_id"
OWNER_NAME_FIELD = "所属流程"
VERSION_FIELD = "流程版本"
KEY_FIELD = "step_key"
CLAIM_STATE_FIELD = "归属状态"

CLAIM_OWNED = "已归属"
CLAIM_UNOWNED = "未归属"

#: 已处理过的步骤行 id：新出现的行（含历史 bot_id 行）会被增量登记，
#: 因此运行中新增的归属也能被识别，而不只是在首次读取时扫描一次。
_seen_ids: set[int] = set()


def _slug(text: str) -> str:
    """把中文/英文步骤名压成可比较的片段，保持键可读且稳定。"""
    cleaned = re.sub(r"\s+", "-", str(text or "").strip())
    cleaned = re.sub(r"[^0-9A-Za-z一-鿿\-]", "", cleaned)
    return cleaned[:24] or "step"


def stable_step_key(step: dict[str, Any]) -> str:
    """返回步骤的稳定键：优先显式键，否则由行 id + 步骤序号确定。

    只用行 id 和步骤序号，不掺入状态/名称等易变字段，
    因此重复读取、重新进入页面得到的键一致。
    """
    existing = str(step.get(KEY_FIELD) or "").strip()
    if existing:
        return existing
    step_id = int(step.get("id", 0))
    seq = str(step.get("步骤序号") or "0").strip()
    name = _slug(step.get("步骤名称", ""))
    return f"SEQ-{seq}-S{step_id}-{name}".upper()


def ensure_step_key(step: dict[str, Any]) -> str:
    """确保步骤行持久化了稳定键。"""
    key = stable_step_key(step)
    step[KEY_FIELD] = key
    return key


def _adopt_legacy_ownership() -> None:
    """把行上的 bot_id 显式归属登记到注册表（幂等，不覆盖冲突）。

    按行 id 增量处理：新插入的行也会被登记，注册表已有的归属不会被覆盖。
    """
    for row in store.rows("step_editor"):
        row_id = int(row.get("id", 0))
        if row_id in _seen_ids:
            continue
        _seen_ids.add(row_id)
        bot_id = row.get(OWNER_ID_FIELD)
        if bot_id is None:
            row.setdefault(CLAIM_STATE_FIELD, CLAIM_UNOWNED)
            ensure_step_key(row)
            continue
        try:
            bot_id = int(bot_id)
        except (TypeError, ValueError):
            row.pop(OWNER_ID_FIELD, None)
            row.setdefault(CLAIM_STATE_FIELD, CLAIM_UNOWNED)
            ensure_step_key(row)
            continue
        if not store.flow_versions.claim(int(row["id"]), bot_id):
            # 注册表里已属于别的流程：以注册表为准，行上的归属不可信
            owner = store.flow_versions.owner_of(int(row["id"]))
            if owner is None:
                row.pop(OWNER_ID_FIELD, None)
                row[CLAIM_STATE_FIELD] = CLAIM_UNOWNED
                ensure_step_key(row)
                continue
            bot_id = owner
            row[OWNER_ID_FIELD] = owner
        row[CLAIM_STATE_FIELD] = CLAIM_OWNED
        ensure_step_key(row)


def normalize_rows() -> None:
    """读取步骤数据前的统一整理：补齐稳定键并登记显式归属。"""
    _adopt_legacy_ownership()


def owner_of(step: dict[str, Any]) -> int | None:
    """步骤的权威归属流程 ID（注册表优先，其次行字段）。"""
    _adopt_legacy_ownership()
    owner = store.flow_versions.owner_of(int(step["id"]))
    if owner is not None:
        return owner
    bot_id = step.get(OWNER_ID_FIELD)
    try:
        return int(bot_id) if bot_id is not None else None
    except (TypeError, ValueError):
        return None


def owned_steps(bot_id: int, *, include_discarded: bool = False) -> list[dict[str, Any]]:
    """列出显式归属到该流程的步骤（不包含任何未归属草稿）。"""
    _adopt_legacy_ownership()
    owned: list[dict[str, Any]] = []
    for row in store.rows("step_editor"):
        if owner_of(row) != int(bot_id):
            continue
        if include_discarded or row.get("status") != "已废弃":
            ensure_step_key(row)
            owned.append(row)
    owned.sort(key=lambda r: (str(r.get("步骤序号", "0")), int(r.get("id", 0))))
    return owned


def unowned_steps() -> list[dict[str, Any]]:
    """列出没有明确归属的步骤。"""
    _adopt_legacy_ownership()
    rows: list[dict[str, Any]] = []
    for row in store.rows("step_editor"):
        if owner_of(row) is not None:
            continue
        ensure_step_key(row)
        rows.append(row)
    return rows


def name_similar(step: dict[str, Any], bot_name: str) -> bool:
    """名称相似仅用于提示待认领，绝不作为归属依据。"""
    name = str(step.get("步骤名称", ""))
    if not bot_name:
        return False
    if bot_name[:4] and bot_name[:4] in name:
        return True
    prefix = 0
    for left, right in zip(name, bot_name):
        if left != right:
            break
        prefix += 1
    return prefix >= 4


def pending_claim_candidates(bot_name: str) -> list[dict[str, Any]]:
    """待认领队列：未归属且名称相似的步骤，供人工确认后显式认领。"""
    return [
        {
            "step_id": int(row["id"]),
            "step_key": stable_step_key(row),
            "name": row.get("步骤名称"),
            "seq": row.get("步骤序号"),
            "status": row.get("status"),
            "suggested_bot_name": bot_name,
            "reason": "名称与流程相似，等待人工确认归属",
        }
        for row in unowned_steps()
        if name_similar(row, bot_name)
    ]


def bind_steps(
    bot_id: int,
    bot_name: str,
    step_ids: list[int],
    version: str | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """显式认领步骤到流程：返回 (成功认领的步骤, 冲突步骤)。

    注册表认领是原子的，冲突步骤保持原归属，绝不会一稿多挂。
    """
    _adopt_legacy_ownership()
    claimed: list[dict[str, Any]] = []
    conflicts: list[dict[str, Any]] = []
    for step_id in step_ids:
        entry = store.find("step_editor", int(step_id))
        if entry is None:
            continue
        if not store.flow_versions.claim(int(step_id), int(bot_id)):
            conflicts.append(entry)
            continue
        entry[OWNER_ID_FIELD] = int(bot_id)
        entry[OWNER_NAME_FIELD] = bot_name
        entry[CLAIM_STATE_FIELD] = CLAIM_OWNED
        if version:
            entry[VERSION_FIELD] = version
        ensure_step_key(entry)
        claimed.append(entry)
    return claimed, conflicts


def release_step(step_id: int) -> bool:
    """释放步骤归属，回到未归属状态（不删除步骤本身）。"""
    entry = store.find("step_editor", int(step_id))
    if entry is None:
        return False
    store.flow_versions.release(int(step_id))
    entry.pop(OWNER_ID_FIELD, None)
    entry[OWNER_NAME_FIELD] = ""
    entry[VERSION_FIELD] = ""
    entry[CLAIM_STATE_FIELD] = CLAIM_UNOWNED
    return True


def claim_step(step_id: int, bot_id: int, bot_name: str) -> dict[str, Any] | None:
    """人工认领：把一个待认领步骤显式挂到某流程，返回步骤行或 None。"""
    claimed, conflicts = bind_steps(int(bot_id), bot_name, [int(step_id)])
    if conflicts or not claimed:
        return None
    return claimed[0]


def board() -> dict[str, Any]:
    """流程编排页数据：已归属步骤、未归属草稿、版本快照。"""
    _adopt_legacy_ownership()
    groups: list[dict[str, Any]] = []
    for bot in store.rows("bot_definition"):
        bot_id = int(bot["id"])
        bot_name = str(bot.get("流程名称") or "")
        steps = owned_steps(bot_id)
        snapshot = store.flow_versions.latest_snapshot(bot_id)
        groups.append({
            "bot_id": bot_id,
            "bot_name": bot_name,
            "bot_status": bot.get("status"),
            "version": snapshot.get("version") if snapshot else bot.get("版本号"),
            "owned_steps": [_step_view(step) for step in steps],
            "unassigned_candidates": pending_claim_candidates(bot_name),
            "snapshot": snapshot,
        })
    unowned = unowned_steps()
    return {
        "groups": groups,
        "unassigned_steps": [_step_view(step) for step in unowned],
        "snapshots": store.flow_versions.list_snapshots(),
        "conflicts": store.flow_versions.list_conflicts(),
        "summary": {
            "owned_steps": len(store.flow_versions.owner_map()),
            "unassigned_steps": len(unowned),
            "snapshots": len(store.flow_versions.list_snapshots()),
            "conflicts": len(store.flow_versions.list_conflicts()),
        },
    }


def _step_view(step: dict[str, Any]) -> dict[str, Any]:
    owner = owner_of(step)
    return {
        "step_id": int(step["id"]),
        "step_key": stable_step_key(step),
        "seq": step.get("步骤序号"),
        "name": step.get("步骤名称"),
        "type": step.get("操作类型"),
        "status": step.get("status"),
        "bot_id": owner,
        "bot_name": step.get(OWNER_NAME_FIELD) or "" if owner is not None else "",
        "version": step.get(VERSION_FIELD) or "",
        "claim_state": CLAIM_OWNED if owner is not None else CLAIM_UNOWNED,
    }
