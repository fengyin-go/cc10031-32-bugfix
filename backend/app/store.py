"""内存数据仓库：给每个业务模块准备一份可筛选、可流转的示例数据。

真实项目里这里会换成数据库访问层；当前实现只依赖标准库，保证克隆下来就能起。

跨模块数据流能力：
- DataChannel: 服务间数据发布/消费通道，支持 topic 订阅、快照读、消费确认
- DirtyDataVault: 脏数据隔离区，存储校验不通过/解析失败的脏数据记录
- FailureRecovery: 失败恢复管理器，记录失败步骤并支持重试
- CollectRecord: 采集记录存储，记录每次采集产出的结构化数据
- FlowVersionRegistry: 步骤归属索引 + 流程版本快照，保证同一步骤只属于一个流程
- ProcessingLedger: 处理项台账，收敛业务清单/结果台账并同步跨业务面待办
"""
from __future__ import annotations

import copy
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from app.seed import SEED_ROWS

# ---------------------------------------------------------------------------
# 跨模块数据通道 (DataChannel) — 服务间发布/消费
# ---------------------------------------------------------------------------

@dataclass
class ChannelMessage:
    """数据通道中的单条消息，带 topic、来源、负载和确认状态。"""
    msg_id: int
    topic: str
    source_module: str
    payload: dict[str, Any]
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    consumed_by: list[str] = field(default_factory=list)
    confirmed: bool = False

    def mark_consumed(self, consumer: str) -> None:
        if consumer not in self.consumed_by:
            self.consumed_by.append(consumer)


class DataChannel:
    """跨模块数据通道：支持 topic 发布/订阅，服务间数据流转的桥梁。

    用法::

        store.channel.publish("rpa.bot_created", "bot_definition", bot_data)
        msgs = store.channel.consume("step_editor", "rpa.bot_created")
        store.channel.confirm(msg_id, "step_editor")
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._seq = 0
        self._messages: list[ChannelMessage] = []

    @property
    def total_messages(self) -> int:
        with self._lock:
            return len(self._messages)

    def publish(self, topic: str, source: str, payload: dict[str, Any]) -> ChannelMessage:
        with self._lock:
            self._seq += 1
            msg = ChannelMessage(
                msg_id=self._seq, topic=topic,
                source_module=source, payload=payload,
            )
            self._messages.append(msg)
            return msg

    def consume(self, consumer: str, topic: str | None = None,
                since_msg_id: int | None = None) -> list[ChannelMessage]:
        """获取指定 consumer 尚未消费的消息。"""
        with self._lock:
            results: list[ChannelMessage] = []
            for msg in self._messages:
                if topic and msg.topic != topic:
                    continue
                if since_msg_id is not None and msg.msg_id <= since_msg_id:
                    continue
                if consumer not in msg.consumed_by:
                    results.append(msg)
                    msg.mark_consumed(consumer)
            return results

    def confirm(self, msg_id: int, consumer: str) -> bool:
        with self._lock:
            for msg in self._messages:
                if msg.msg_id == msg_id:
                    msg.confirmed = True
                    return True
            return False

    def snapshot(self, topic: str | None = None) -> list[dict[str, Any]]:
        """返回消息快照，供跨模块分析/看板消费。"""
        with self._lock:
            msgs = self._messages if topic is None else [
                m for m in self._messages if m.topic == topic
            ]
            return [{
                "msg_id": m.msg_id, "topic": m.topic,
                "source": m.source_module, "payload": m.payload,
                "created_at": m.created_at, "consumed_by": m.consumed_by,
                "confirmed": m.confirmed,
            } for m in msgs]

    def latest(self, topic: str) -> dict[str, Any] | None:
        with self._lock:
            for msg in reversed(self._messages):
                if msg.topic == topic:
                    return {
                        "msg_id": msg.msg_id, "topic": msg.topic,
                        "source": msg.source_module, "payload": msg.payload,
                        "created_at": msg.created_at, "confirmed": msg.confirmed,
                    }
            return None

    def uncomfirmed_count(self) -> int:
        with self._lock:
            return sum(1 for m in self._messages if not m.confirmed)


# ---------------------------------------------------------------------------
# 脏数据隔离区 (DirtyDataVault)
# ---------------------------------------------------------------------------

@dataclass
class DirtyRecord:
    """脏数据记录：校验失败或解析异常的数据行。"""
    record_id: int
    source_module: str
    batch_ref: str
    raw_data: dict[str, Any]
    reason: str  # 如 "字段缺失", "格式非法", "解析超时"
    detected_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    quarantined: bool = True
    recovered_to: str | None = None


class DirtyDataVault:
    """脏数据隔离区：被校验规则或解析器标记为问题数据后移入此处。

    用法::

        vault = store.dirty_vault
        vault.quarantine("data_extract", "BATC-001", row, "缺少必填字段")
        dirty = vault.list_by_source("data_extract")
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._seq = 0
        self._records: list[DirtyRecord] = []

    @property
    def count(self) -> int:
        with self._lock:
            return len(self._records)

    def quarantine(self, source: str, batch_ref: str,
                   raw_data: dict[str, Any], reason: str) -> DirtyRecord:
        with self._lock:
            self._seq += 1
            rec = DirtyRecord(
                record_id=self._seq, source_module=source,
                batch_ref=batch_ref, raw_data=raw_data, reason=reason,
            )
            self._records.append(rec)
            return rec

    def list_by_source(self, source: str) -> list[DirtyRecord]:
        with self._lock:
            return [r for r in self._records if r.source_module == source]

    def list_by_batch(self, batch_ref: str) -> list[DirtyRecord]:
        with self._lock:
            return [r for r in self._records if r.batch_ref == batch_ref]

    def mark_recovered(self, record_id: int, recovered_to: str) -> bool:
        with self._lock:
            for r in self._records:
                if r.record_id == record_id:
                    r.quarantined = False
                    r.recovered_to = recovered_to
                    return True
            return False

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            quarantined = sum(1 for r in self._records if r.quarantined)
            by_reason: dict[str, int] = {}
            for r in self._records:
                by_reason[r.reason] = by_reason.get(r.reason, 0) + 1
            return {
                "total": len(self._records),
                "quarantined": quarantined,
                "recovered": len(self._records) - quarantined,
                "by_reason": by_reason,
            }


# ---------------------------------------------------------------------------
# 采集记录 (CollectRecord) — 每次采集产出的结构化数据
# ---------------------------------------------------------------------------

@dataclass
class CollectEntry:
    entry_id: int
    batch_ref: str
    source_module: str
    fields: dict[str, Any]
    collected_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    validated: bool = False
    quality_score: float = 1.0


class CollectRecordStore:
    """采集记录存储：每次采集流程产出的结构化数据行。

    用法::

        rec = store.collect_records.add("BATC-001", "data_extract", {"字段1": "值1", ...})
        store.collect_records.mark_validated(rec.entry_id)
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._seq = 0
        self._entries: list[CollectEntry] = []

    @property
    def count(self) -> int:
        with self._lock:
            return len(self._entries)

    def add(self, batch_ref: str, source: str,
            fields: dict[str, Any]) -> CollectEntry:
        with self._lock:
            self._seq += 1
            entry = CollectEntry(
                entry_id=self._seq, batch_ref=batch_ref,
                source_module=source, fields=dict(fields),
            )
            self._entries.append(entry)
            return entry

    def list_by_batch(self, batch_ref: str) -> list[CollectEntry]:
        with self._lock:
            return [e for e in self._entries if e.batch_ref == batch_ref]

    def list_by_source(self, source: str) -> list[CollectEntry]:
        with self._lock:
            return [e for e in self._entries if e.source_module == source]

    def mark_validated(self, entry_id: int, score: float = 1.0) -> bool:
        with self._lock:
            for e in self._entries:
                if e.entry_id == entry_id:
                    e.validated = True
                    e.quality_score = score
                    return True
            return False

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            validated = sum(1 for e in self._entries if e.validated)
            by_source: dict[str, int] = {}
            for e in self._entries:
                by_source[e.source_module] = by_source.get(e.source_module, 0) + 1
            return {
                "total": len(self._entries),
                "validated": validated,
                "unvalidated": len(self._entries) - validated,
                "by_source": by_source,
            }


# ---------------------------------------------------------------------------
# 失败恢复管理 (FailureRecovery)
# ---------------------------------------------------------------------------

@dataclass
class RecoveryEntry:
    recovery_id: int
    module: str
    batch_ref: str
    step: str
    error_detail: str
    attempt: int = 0
    max_attempts: int = 3
    resolved: bool = False
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


class FailureRecovery:
    """失败恢复管理器：记录失败步骤并支持自动重试。

    用法::

        rec = store.recovery.log("step_editor", "BATC-001", "元素定位",
                                  "选择器超时未匹配")
        store.recovery.retry(rec.recovery_id)
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._seq = 0
        self._entries: list[RecoveryEntry] = []

    @property
    def count(self) -> int:
        with self._lock:
            return len(self._entries)

    def log(self, module: str, batch_ref: str, step: str,
            error_detail: str, max_attempts: int = 3) -> RecoveryEntry:
        with self._lock:
            self._seq += 1
            entry = RecoveryEntry(
                recovery_id=self._seq, module=module,
                batch_ref=batch_ref, step=step,
                error_detail=error_detail, max_attempts=max_attempts,
            )
            self._entries.append(entry)
            return entry

    def retry(self, recovery_id: int) -> tuple[bool, str]:
        with self._lock:
            for e in self._entries:
                if e.recovery_id == recovery_id:
                    if e.resolved:
                        return False, "已恢复，无需重试"
                    e.attempt += 1
                    if e.attempt > e.max_attempts:
                        return False, f"已达最大重试次数 {e.max_attempts}"
                    return True, f"重试 {e.attempt}/{e.max_attempts}"
            return False, "恢复记录不存在"

    def resolve(self, recovery_id: int) -> bool:
        with self._lock:
            for e in self._entries:
                if e.recovery_id == recovery_id:
                    e.resolved = True
                    return True
            return False

    def pending_count(self) -> int:
        with self._lock:
            return sum(1 for e in self._entries if not e.resolved)

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            pending = sum(1 for e in self._entries if not e.resolved)
            return {
                "total": len(self._entries),
                "resolved": len(self._entries) - pending,
                "pending": pending,
            }


# ---------------------------------------------------------------------------
# 流程版本台账 (FlowVersionRegistry) — 步骤归属 + 版本快照 + 发布幂等
# ---------------------------------------------------------------------------

#: 流程版本编排的 32 号处理终态：归属冲突时版本快照落到该终态。
CONFLICT_TERMINAL_CODE = 32
TERMINAL_PUBLISHED = "已发布"
TERMINAL_CONFLICT = "编排冲突"


class FlowVersionRegistry:
    """流程版本台账：持久化步骤归属索引和流程版本快照。

    职责边界：
    - 步骤归属唯一：一个步骤行只能被一个流程占用，认领是原子的；
    - 版本快照：一次成功发布生成一条不可变快照，记录稳定步骤键；
    - 发布幂等：同一发布键并发重复写入只接受一次，后续请求返回原结果；
    - 冲突终态：归属冲突的发布不会写入第二个流程，而是记录 32 号冲突终态。

    数据保存进程内存中（与 store 一致），页面刷新重新进入后读到的
    仍是同一份快照和归属索引。
    """

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._seq = 0
        self._step_owner: dict[int, int] = {}
        self._snapshots: list[dict[str, Any]] = []
        self._by_bot_version: dict[tuple[int, str], dict[str, Any]] = {}
        self._by_publish_key: dict[str, dict[str, Any]] = {}
        self._conflicts: list[dict[str, Any]] = []
        self._claims: list[dict[str, Any]] = []

    # ---- 步骤归属 ----

    def owner_of(self, step_id: int) -> int | None:
        with self._lock:
            return self._step_owner.get(int(step_id))

    def owner_map(self) -> dict[int, int]:
        with self._lock:
            return dict(self._step_owner)

    def claim(self, step_id: int, bot_id: int) -> bool:
        """原子认领：步骤已被其他流程占用时返回 False，绝不改挂。"""
        with self._lock:
            current = self._step_owner.get(int(step_id))
            if current is not None and current != int(bot_id):
                return False
            self._step_owner[int(step_id)] = int(bot_id)
            return True

    def release(self, step_id: int) -> None:
        with self._lock:
            self._step_owner.pop(int(step_id), None)

    def owned_step_ids(self, bot_id: int) -> list[int]:
        with self._lock:
            return sorted(
                sid for sid, owner in self._step_owner.items() if owner == int(bot_id)
            )

    # ---- 版本快照 ----

    def list_snapshots(self, bot_id: int | None = None) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._snapshots if bot_id is None else [
                s for s in self._snapshots if s["bot_id"] == int(bot_id)
            ]
            return [copy.deepcopy(row) for row in rows]

    def latest_snapshot(self, bot_id: int) -> dict[str, Any] | None:
        with self._lock:
            for snapshot in reversed(self._snapshots):
                if snapshot["bot_id"] == int(bot_id) and snapshot.get("terminal_state") == TERMINAL_PUBLISHED:
                    return copy.deepcopy(snapshot)
            return None

    def snapshot_for_version(self, bot_id: int, version: str | None) -> dict[str, Any] | None:
        """按流程 + 版本取快照；版本为空时取最新已发布快照。"""
        if not version:
            return self.latest_snapshot(bot_id)
        with self._lock:
            for snapshot in reversed(self._snapshots):
                if (snapshot["bot_id"] == int(bot_id)
                        and str(snapshot["version"]) == str(version)
                        and snapshot.get("terminal_state") == TERMINAL_PUBLISHED):
                    return copy.deepcopy(snapshot)
            return None

    def snapshot_by_id(self, snapshot_id: int) -> dict[str, Any] | None:
        with self._lock:
            for snapshot in self._snapshots:
                if snapshot["snapshot_id"] == int(snapshot_id):
                    return copy.deepcopy(snapshot)
            return None

    def attach_message(self, snapshot_id: int, msg_id: int) -> None:
        """把发布时产生的通道消息号回填到快照，便于重新进入后追溯。"""
        with self._lock:
            for snapshot in self._snapshots:
                if snapshot["snapshot_id"] == int(snapshot_id):
                    snapshot["msg_id"] = int(msg_id)
                    self._by_publish_key[snapshot["publish_key"]]["msg_id"] = int(msg_id)
                    return

    def next_version(self, bot_id: int, current_version: str | None = None) -> str:
        """生成流程的下一个版本号：首次发布沿用当前版本，之后递增 patch 位。"""
        with self._lock:
            latest = self.latest_snapshot(bot_id)
        if latest is None:
            raw = str(current_version or "").strip()
            numbers = self._version_numbers(raw)
            if numbers is None:
                return "1.0.0"
            return ".".join(str(n) for n in numbers)
        numbers = self._version_numbers(str(latest.get("version")))
        if numbers is None:
            return "1.0.1"
        numbers[2] += 1
        return ".".join(str(n) for n in numbers)

    @staticmethod
    def _version_numbers(raw: str) -> list[int] | None:
        parts = raw.split(".") if raw else []
        if not parts or len(parts) > 3:
            return None
        try:
            numbers = [int(p) for p in parts]
        except ValueError:
            return None
        while len(numbers) < 3:
            numbers.append(0)
        return numbers

    @classmethod
    def normalize_version(cls, raw: Any) -> str:
        """把外部给来的版本号收敛成 x.y.z；非法值回落到 1.0.0。"""
        numbers = cls._version_numbers(str(raw or "").strip())
        if numbers is None:
            return "1.0.0"
        return ".".join(str(n) for n in numbers)

    @staticmethod
    def make_publish_key(bot_id: int, base_version: str | None,
                         step_keys: list[str],
                         fingerprint: str | None = None) -> str:
        """发布幂等键：同一流程的同一份步骤内容只生成一个版本。

        给了 fingerprint 时按“流程 + 内容指纹”收敛，因此刷新页面后
        再次发布同一份内容会返回原版本；内容改过才会生成新版本。
        没给指纹时退化为“流程 + 基础版本 + 步骤键集合”，顺序无关。
        """
        if fingerprint:
            return f"bot:{int(bot_id)}|fp:{fingerprint}"
        joined = ",".join(sorted(str(key) for key in step_keys))
        return f"bot:{int(bot_id)}|base:{base_version or '-'}|steps:{joined}"

    def publish_version(
        self,
        *,
        publish_key: str,
        bot_id: int,
        bot_name: str,
        version: str,
        base_version: str | None,
        steps: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """原子登记一次流程版本发布，返回统一结果。

        Args:
            publish_key: 幂等键（流程 + 基础版本 + 步骤键集合）
            bot_id / bot_name / version: 发布主体与目标版本
            base_version: 发布前版本，用于识别并发写入
            steps: 本次待归属的步骤行（需含稳定步骤键）

        返回：
            {"status": "created|replayed|conflict", "snapshot": ..., "message": ...}
        """
        with self._lock:
            existing = self._by_publish_key.get(publish_key)
            if existing is not None:
                return {
                    "status": "replayed",
                    "snapshot": copy.deepcopy(existing),
                    "message": "同一发布请求已受理，返回既有版本快照",
                }

            owner_bot = (int(bot_id), str(version))
            if owner_bot in self._by_bot_version:
                held = self._by_bot_version[owner_bot]
                if held.get("publish_key") == publish_key:
                    return {
                        "status": "replayed",
                        "snapshot": copy.deepcopy(held),
                        "message": "同一版本与同一批步骤已受理，返回既有版本快照",
                    }
                record = {
                    "bot_id": int(bot_id),
                    "bot_name": bot_name,
                    "version": version,
                    "base_version": base_version,
                    "conflict_step_ids": [],
                    "terminal_state": TERMINAL_CONFLICT,
                    "terminal_code": CONFLICT_TERMINAL_CODE,
                    "message": f"版本 {version} 已被占用，并发写入只接受首个版本",
                    "created_at": datetime.now(timezone.utc).isoformat(),
                }
                self._conflicts.append(record)
                return {
                    "status": "conflict",
                    "snapshot": copy.deepcopy(held),
                    "conflict": copy.deepcopy(record),
                    "message": record["message"],
                }

            claimed: list[dict[str, Any]] = []
            skipped: list[dict[str, Any]] = []
            conflicts: list[int] = []
            # 先整批检查归属，任何一个步骤已归属别的流程就整批拒绝，
            # 不做“部分写入”，避免同一版本里混入他人步骤。
            for step in steps:
                step_id = int(step["id"])
                current = self._step_owner.get(step_id)
                if current is not None and current != int(bot_id):
                    skipped.append(step)
                    conflicts.append(step_id)
            if conflicts:
                record = {
                    "bot_id": int(bot_id),
                    "bot_name": bot_name,
                    "version": version,
                    "base_version": base_version,
                    "conflict_step_ids": conflicts,
                    "skipped_step_ids": [int(s["id"]) for s in skipped],
                    "terminal_state": TERMINAL_CONFLICT,
                    "terminal_code": CONFLICT_TERMINAL_CODE,
                    "message": "待发布步骤已归属其他流程，版本未写入",
                    "created_at": datetime.now(timezone.utc).isoformat(),
                }
                self._conflicts.append(record)
                return {
                    "status": "conflict",
                    "snapshot": None,
                    "skipped": [int(s["id"]) for s in skipped],
                    "conflict": copy.deepcopy(record),
                    "message": record["message"],
                }

            for step in steps:
                step_id = int(step["id"])
                self._step_owner[step_id] = int(bot_id)
                claimed.append(step)

            self._seq += 1
            snapshot = {
                "snapshot_id": self._seq,
                "snapshot_no": self._seq,
                "bot_id": int(bot_id),
                "bot_name": bot_name,
                "version": str(version),
                "base_version": base_version,
                "publish_key": publish_key,
                "step_ids": [int(s["id"]) for s in claimed],
                "step_keys": [s.get("step_key") or f"step-{s['id']}" for s in claimed],
                "steps": [
                    {
                        "step_id": int(s["id"]),
                        "step_key": s.get("step_key") or f"step-{s['id']}",
                        "seq": s.get("步骤序号"),
                        "name": s.get("步骤名称"),
                        "type": s.get("操作类型"),
                        "element": s.get("目标元素"),
                        "status": s.get("status"),
                    }
                    for s in claimed
                ],
                "skipped_step_ids": [int(s["id"]) for s in skipped],
                "conflict_step_ids": conflicts,
                "terminal_state": TERMINAL_PUBLISHED if not conflicts else TERMINAL_CONFLICT,
                "terminal_code": None if not conflicts else CONFLICT_TERMINAL_CODE,
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
            self._snapshots.append(snapshot)
            self._by_bot_version[owner_bot] = snapshot
            self._by_publish_key[publish_key] = snapshot
            return {
                "status": "created",
                "snapshot": copy.deepcopy(snapshot),
                "skipped": [int(s["id"]) for s in skipped],
                "message": f"流程版本 {version} 已生成快照，归属步骤 {len(claimed)} 个",
            }

    def list_conflicts(self) -> list[dict[str, Any]]:
        with self._lock:
            return copy.deepcopy(self._conflicts)

    def revise_step(self, step_id: int, bot_id: int, changes: dict[str, Any]) -> dict[str, Any] | None:
        """修改某个流程已归属步骤的定义。

        只允许归属流程本人修改；改完后旧版本快照保留原样，
        新版本需重新发布后才会生成，满足“再次返回后持久保留原结果”。
        """
        with self._lock:
            current = self._step_owner.get(int(step_id))
            if current is None or current != int(bot_id):
                return None
            snapshot_owner = int(bot_id)
        entry = store.find("step_editor", int(step_id))
        if entry is None:
            return None
        for field, value in changes.items():
            if field in ("id", "status", "pending", "abnormal"):
                continue
            entry[field] = value
        entry["_revised_by"] = snapshot_owner
        return entry

    def add_claim(self, record: dict[str, Any]) -> dict[str, Any]:
        with self._lock:
            self._claims.append(copy.deepcopy(record))
            return copy.deepcopy(record)

    def list_claims(self, bot_id: int | None = None) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._claims if bot_id is None else [
                c for c in self._claims if c.get("bot_id") == int(bot_id)
            ]
            return copy.deepcopy(rows)

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "snapshots": len(self._snapshots),
                "owned_steps": len(self._step_owner),
                "conflicts": len(self._conflicts),
                "latest_snapshot_id": self._seq,
            }


# ---------------------------------------------------------------------------
# 处理项台账 (ProcessingLedger) — 业务清单与结果台账一致、跨业务面待办
# ---------------------------------------------------------------------------

TODO_PENDING = "待处理"
TODO_DONE = "已完成"


class ProcessingLedger:
    """处理项台账：一次“发布流程版本”就是一个处理项。

    同一份结果同时出现在三个业务面：
    - 业务清单（流程定义）：流程状态/版本号；
    - 步骤编排：已归属步骤与版本快照；
    - 任务队列：按稳定步骤键排出的执行项。
    台账负责把三面写入收敛在一个事务里，并提供待办同步与一致性核对。
    """

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._items: dict[str, dict[str, Any]] = {}
        self._todos: dict[str, list[dict[str, Any]]] = {}
        self._seq = 0

    def begin(self, item_key: str, *, bot_id: int, bot_name: str,
              requested_version: str | None, base_version: str | None) -> dict[str, Any]:
        """登记处理项（幂等）：同一键再次进入返回原结果，不产生第二个版本。"""
        with self._lock:
            existing = self._items.get(item_key)
            if existing is not None:
                return copy.deepcopy(existing)
            self._seq += 1
            item = {
                "item_id": self._seq,
                "item_key": item_key,
                "bot_id": int(bot_id),
                "bot_name": bot_name,
                "requested_version": requested_version,
                "base_version": base_version,
                "state": "处理中",
                "result": None,
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
            self._items[item_key] = item
            return copy.deepcopy(item)

    def commit(self, item_key: str, result: dict[str, Any]) -> dict[str, Any]:
        with self._lock:
            item = self._items.get(item_key)
            if item is None:
                return {}
            if item.get("result") is not None:
                # 已固化结果：再次返回时原样返回，保留首次结果
                return copy.deepcopy(item["result"])
            item["result"] = copy.deepcopy(result)
            item["state"] = "已处理" if result.get("published") else "处理失败"
            item["finished_at"] = datetime.now(timezone.utc).isoformat()
            return copy.deepcopy(item["result"])

    def abort(self, item_key: str, message: str) -> None:
        with self._lock:
            item = self._items.get(item_key)
            if item is not None and item.get("result") is None:
                item["state"] = "处理失败"
                item["error"] = message
                item["finished_at"] = datetime.now(timezone.utc).isoformat()

    def get(self, item_key: str) -> dict[str, Any] | None:
        with self._lock:
            item = self._items.get(item_key)
            return copy.deepcopy(item) if item else None

    def list_items(self, bot_id: int | None = None) -> list[dict[str, Any]]:
        with self._lock:
            rows = [
                copy.deepcopy(item) for item in self._items.values()
                if bot_id is None or item["bot_id"] == int(bot_id)
            ]
        return sorted(rows, key=lambda r: r["item_id"])

    # ---- 跨业务面待办 ----

    def sync_todo(self, surface: str, ref_id: str, *, bot_id: int,
                  title: str, state: str = TODO_PENDING,
                  detail: dict[str, Any] | None = None) -> dict[str, Any]:
        """写入/更新某个业务面的待办，同一 (surface, ref) 只有一条。"""
        with self._lock:
            bucket = self._todos.setdefault(surface, [])
            for todo in bucket:
                if todo["ref_id"] == ref_id:
                    todo["state"] = state
                    todo["title"] = title
                    todo["updated_at"] = datetime.now(timezone.utc).isoformat()
                    if detail is not None:
                        todo["detail"] = copy.deepcopy(detail)
                    return copy.deepcopy(todo)
            todo = {
                "surface": surface,
                "ref_id": ref_id,
                "bot_id": int(bot_id),
                "title": title,
                "state": state,
                "detail": copy.deepcopy(detail or {}),
                "created_at": datetime.now(timezone.utc).isoformat(),
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
            bucket.append(todo)
            return copy.deepcopy(todo)

    def todos(self, surface: str | None = None) -> list[dict[str, Any]]:
        with self._lock:
            if surface:
                return copy.deepcopy(self._todos.get(surface, []))
            return [
                copy.deepcopy(todo)
                for bucket in self._todos.values() for todo in bucket
            ]

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "items": len(self._items),
                "processed": sum(1 for i in self._items.values() if i["state"] == "已处理"),
                "failed": sum(1 for i in self._items.values() if i["state"] == "处理失败"),
                "todos_pending": sum(
                    1 for bucket in self._todos.values()
                    for todo in bucket if todo["state"] == TODO_PENDING
                ),
            }


# ---------------------------------------------------------------------------
# 增强版 Store — 整合通道、脏数据、采集记录、恢复
# ---------------------------------------------------------------------------

class Store:
    def __init__(self) -> None:
        self._tables: dict[str, list[dict[str, Any]]] = {
            name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()
        }
        # 跨模块基础设施
        self.channel = DataChannel()
        self.dirty_vault = DirtyDataVault()
        self.collect_records = CollectRecordStore()
        self.recovery = FailureRecovery()
        # 流程版本台账：步骤归属索引 + 版本快照（跨页面/刷新持久）
        self.flow_versions = FlowVersionRegistry()
        # 处理项台账：发布结果 + 跨业务面待办
        self.ledger = ProcessingLedger()

    # ---- 基础数据操作 ----

    def module_names(self) -> list[str]:
        return sorted(self._tables)

    def rows(self, module: str) -> list[dict[str, Any]]:
        return self._tables.setdefault(module, [])

    def find(self, module: str, entry_id: int) -> dict[str, Any] | None:
        for row in self.rows(module):
            if int(row.get("id", 0)) == entry_id:
                return row
        return None

    def overview(self) -> dict[str, object]:
        modules: list[dict[str, object]] = []
        for name in self.module_names():
            rows = self.rows(name)
            modules.append({
                "name": name,
                "created": len(rows),
                "pending": sum(1 for row in rows if row.get("pending")),
                "abnormal": sum(1 for row in rows if row.get("abnormal")),
            })
        cards = [
            {"label": "业务模块", "value": len(modules)},
            {"label": "今日新增", "value": sum(int(item["created"]) for item in modules)},
            {"label": "待处理", "value": sum(int(item["pending"]) for item in modules)},
            {"label": "异常量", "value": sum(int(item["abnormal"]) for item in modules)},
        ]
        return {"cards": cards, "modules": modules}


store = Store()