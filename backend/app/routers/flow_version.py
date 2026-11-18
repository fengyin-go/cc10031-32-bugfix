"""流程版本与步骤归属接口：编排页读取归属/快照，发布与认领动作都走这里。

路径前缀 /api/flow-versions，避开 /api/{module} 动态路由，
保证资源路由注册顺序变化时也不会被模块名抢占。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from app.schemas import ActionResult
from app.services import step_ownership
from app.services.bot_definition import BotDefinitionService
from app.services.step_editor import StepEditorService
from app.services.task_queue import TaskQueueService
from app.store import store

router = APIRouter(prefix="/api/flow-versions", tags=["流程版本编排"])


class PublishPayload(BaseModel):
    bot_id: int
    version: str | None = None
    publish_key: str | None = None
    step_ids: list[int] = Field(default_factory=list)


class ClaimPayload(BaseModel):
    step_id: int
    bot_id: int


class ReleasePayload(BaseModel):
    step_id: int
    reason: str | None = None


@router.get("")
def flow_version_board(bot_id: int | None = Query(default=None)) -> dict[str, Any]:
    """编排页数据：已归属步骤、未归属草稿、待认领队列、版本快照。"""
    board = step_ownership.board()
    if bot_id is not None:
        board["groups"] = [
            group for group in board["groups"] if group["bot_id"] == int(bot_id)
        ]
    return board


@router.get("/snapshots")
def list_snapshots(bot_id: int | None = Query(default=None)) -> dict[str, Any]:
    """版本快照列表：刷新/重新进入后读到的是同一批快照。"""
    rows = store.flow_versions.list_snapshots(bot_id)
    return {"items": rows, "total": len(rows)}


@router.get("/snapshots/{snapshot_id}")
def get_snapshot(snapshot_id: int) -> dict[str, Any]:
    snapshot = store.flow_versions.snapshot_by_id(snapshot_id)
    if snapshot is None:
        raise HTTPException(status_code=404, detail=f"版本快照 {snapshot_id} 不存在")
    return snapshot


@router.get("/unassigned")
def list_unassigned() -> dict[str, Any]:
    """未归属草稿：没有明确归属，不会被任何版本引用。"""
    rows = step_ownership.unowned_steps()
    return {
        "items": [
            {
                "step_id": int(row["id"]),
                "step_key": step_ownership.stable_step_key(row),
                "seq": row.get("步骤序号"),
                "name": row.get("步骤名称"),
                "status": row.get("status"),
            }
            for row in rows
        ],
        "total": len(rows),
    }


@router.get("/pending-claims")
def list_pending_claims(bot_id: int = Query(...)) -> dict[str, Any]:
    """待认领队列：名称与流程相似但仍未归属的步骤，只提示不自动认领。"""
    bot = store.find("bot_definition", int(bot_id))
    if bot is None:
        raise HTTPException(status_code=404, detail=f"流程 {bot_id} 不存在")
    items = step_ownership.pending_claim_candidates(bot.get("流程名称", ""))
    return {"bot_id": int(bot_id), "items": items, "total": len(items)}


@router.get("/execution-plan")
def execution_plan(bot_id: int = Query(...),
                   version: str | None = Query(default=None)) -> dict[str, Any]:
    """按稳定步骤键为已归属步骤生成执行计划（只引用版本快照）。"""
    plan = TaskQueueService().build_execution_plan(int(bot_id), version)
    if plan is None:
        raise HTTPException(
            status_code=404,
            detail=f"流程 {bot_id} 尚无已发布版本快照，无法生成执行计划",
        )
    return plan


@router.post("/publish", response_model=ActionResult)
def publish_flow(payload: PublishPayload) -> ActionResult:
    """发布流程版本：显式归属步骤进入快照，未归属草稿被排除。

    step_ids 是人工确认归属的步骤（如刚认领的待认领项），
    其中任何一个被别的流程占用时整版拒绝并落到 32 号处理终态。
    """
    service = BotDefinitionService()
    result = service.publish_bot(
        int(payload.bot_id), include_step_ids=payload.step_ids or None)
    if result is None:
        return ActionResult(ok=False, message=f"流程 {payload.bot_id} 不存在或当前状态不可发布")
    if not result.get("published"):
        return ActionResult(
            ok=False,
            message=result.get("message", "流程版本发布失败"),
            entry=result.get("conflict"),
        )
    return ActionResult(
        ok=True,
        message=f"流程版本 {result['version']} 已发布，归属步骤 {result['linked_steps']} 个",
        entry=result,
    )


@router.post("/claim", response_model=ActionResult)
def claim_step(payload: ClaimPayload) -> ActionResult:
    """人工认领待认领步骤：写入唯一归属，冲突时拒绝。"""
    service = StepEditorService()
    entry = service.claim_unowned_step(int(payload.step_id), int(payload.bot_id))
    if entry is None:
        return ActionResult(
            ok=False,
            message="认领失败：步骤不存在或已归属其他流程（同一步骤不能属于多个流程）",
        )
    return ActionResult(
        ok=True,
        message=f"步骤 {payload.step_id} 已归属流程 {payload.bot_id}",
        entry={
            "step_id": int(entry["id"]),
            "step_key": step_ownership.stable_step_key(entry),
            "bot_id": entry.get(step_ownership.OWNER_ID_FIELD),
            "version": entry.get(step_ownership.VERSION_FIELD),
        },
    )


@router.post("/release", response_model=ActionResult)
def release_step(payload: ReleasePayload) -> ActionResult:
    """释放归属：把步骤退回未归属池，供其他流程重新认领。"""
    ok = step_ownership.release_step(int(payload.step_id))
    if not ok:
        return ActionResult(ok=False, message=f"步骤 {payload.step_id} 不存在")
    return ActionResult(ok=True, message=f"步骤 {payload.step_id} 已退回未归属")


@router.get("/integrity")
def integrity_check() -> dict[str, Any]:
    """核对归属完整性：同一步骤不得同时出现在多个流程版本快照中。"""
    seen: dict[int, list[int]] = {}
    for snapshot in store.flow_versions.list_snapshots():
        for step_id in snapshot.get("step_ids", []):
            seen.setdefault(int(step_id), []).append(int(snapshot["bot_id"]))
    duplicated = {
        str(step_id): bots for step_id, bots in seen.items() if len(set(bots)) > 1
    }
    owner_map = store.flow_versions.owner_map()
    mismatched = [
        int(step_id)
        for step_id, owners in seen.items()
        if owner_map.get(int(step_id)) is not None
        and owner_map.get(int(step_id)) not in set(owners)
    ]
    return {
        "ok": not duplicated and not mismatched,
        "multi_owner_steps": duplicated,
        "owner_mismatch_steps": mismatched,
        "owned_steps": len(owner_map),
        "snapshots": len(store.flow_versions.list_snapshots()),
        "conflicts": len(store.flow_versions.list_conflicts()),
    }


@router.get("/ledger")
def ledger(bot_id: int | None = Query(default=None)) -> dict[str, Any]:
    """处理项台账：业务清单与结果台账是否一致，跨业务面待办是否同步。"""
    items = store.ledger.list_items(bot_id)
    board_snapshot = step_ownership.board()

    # 业务清单：流程定义行上的版本/状态
    business_rows = {
        int(row["id"]): {
            "status": row.get("status"),
            "version": row.get("版本号") or row.get("version"),
            "terminal": row.get("编排终态"),
        }
        for row in store.rows("bot_definition")
        if bot_id is None or int(row["id"]) == int(bot_id)
    }

    results = {
        item["bot_id"]: item.get("result")
        for item in items if item.get("result") is not None
    }
    drift = []
    for owner_id, row in business_rows.items():
        result = results.get(owner_id)
        if result is None:
            continue
        if bool(result.get("published")):
            if row["status"] != "已发布" or row["version"] != result.get("version"):
                drift.append({
                    "bot_id": owner_id,
                    "business": row,
                    "ledger": {
                        "status": result.get("status"),
                        "version": result.get("version"),
                    },
                })
        elif row["status"] == "已发布":
            drift.append({
                "bot_id": owner_id,
                "business": row,
                "ledger": {"published": False},
            })

    return {
        "ok": not drift,
        "items": items,
        "business_rows": {str(k): v for k, v in business_rows.items()},
        "drift": drift,
        "todos": store.ledger.todos(),
        "pending_todos": store.ledger.snapshot()["todos_pending"],
        "summary": board_snapshot["summary"],
    }

