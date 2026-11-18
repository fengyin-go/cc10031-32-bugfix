"""跨模块数据流编排：RPA 流程从触发到结果输出的完整贯通链路。

每条链路都是一个从入口模块出发、消费关联模块数据、
产出可追踪状态的函数。所有函数返回 PipelineResult，
调用方可以据此判断链路是否完整、何处断流、是否有脏数据。
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any

from app.services import step_ownership
from app.store import store


# ---------------------------------------------------------------------------
# 基础数据结构
# ---------------------------------------------------------------------------

@dataclass
class PipelineResult:
    """链路执行结果：串联多个模块的产出汇总。"""
    pipeline: str
    ok: bool
    trace: list[str] = field(default_factory=list)
    data: dict[str, Any] = field(default_factory=dict)
    dirty: list[dict[str, Any]] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    wait_required: bool = False

    def add_step(self, module: str, entry: dict[str, Any] | None) -> None:
        self.trace.append(f"{module}:{entry.get('id','?')}" if entry else f"{module}:miss")

    def add_dirty(self, entry: dict[str, Any], reason: str) -> None:
        self.dirty.append({"entry": entry, "reason": reason})

    def add_error(self, msg: str) -> None:
        self.errors.append(msg)

    @property
    def summary(self) -> dict[str, Any]:
        return {
            "pipeline": self.pipeline,
            "ok": self.ok,
            "trace_len": len(self.trace),
            "data_keys": list(self.data.keys()),
            "dirty_count": len(self.dirty),
            "error_count": len(self.errors),
            "wait_required": self.wait_required,
        }


# ---------------------------------------------------------------------------
# 脏数据隔离容器
# ---------------------------------------------------------------------------

class DirtyDataVault:
    """脏数据隔离容器：收集链路中产出异常的条目，支持手动清理和批量移除。"""

    def __init__(self) -> None:
        self._records: list[dict[str, Any]] = []

    def isolate(self, entry: dict[str, Any], reason: str, source_module: str) -> None:
        self._records.append({
            "entry": dict(entry),
            "reason": reason,
            "source": source_module,
            "isolated_at": time.time(),
        })

    def list_all(self) -> list[dict[str, Any]]:
        return list(self._records)

    def purge(self, entry_id: int) -> int:
        before = len(self._records)
        self._records = [
            r for r in self._records
            if int(r["entry"].get("id", 0)) != entry_id
        ]
        return before - len(self._records)

    def size(self) -> int:
        return len(self._records)


# ---------------------------------------------------------------------------
# 失败恢复机制
# ---------------------------------------------------------------------------

class RecoveryManager:
    """基于重试策略的失败恢复器。"""

    def __init__(self, max_retries: int = 3, backoff_base: float = 2.0):
        self.max_retries = max_retries
        self.backoff_base = backoff_base
        self._history: list[dict[str, Any]] = []

    def attempt(
        self,
        operation: str,
        fn,
        *args: Any,
        **kwargs: Any,
    ) -> tuple[bool, Any, str]:
        """执行带重试的动作，返回 (成功, 结果, 错误消息)。"""
        last_error = ""
        for trial in range(1, self.max_retries + 1):
            try:
                result = fn(*args, **kwargs)
                self._history.append({
                    "operation": operation,
                    "trial": trial,
                    "success": True,
                })
                return True, result, ""
            except Exception as exc:  # noqa: BLE001
                last_error = str(exc)
                self._history.append({
                    "operation": operation,
                    "trial": trial,
                    "success": False,
                    "error": last_error,
                })
                if trial < self.max_retries:
                    time.sleep(self.backoff_base ** (trial - 1))
        return False, None, last_error

    def history(self) -> list[dict[str, Any]]:
        return list(self._history)


# ---------------------------------------------------------------------------
# 链路 1: bot_definition → step_editor → execution_log → batch_tracker
# ---------------------------------------------------------------------------

def pipeline_rpa_lifecycle(bot_id: int | None = None) -> PipelineResult:
    """RPA 流程全生命周期链路：流程定义 → 步骤编排 → 执行记录 → 批次追踪。"""
    result = PipelineResult(pipeline="bot_definition→step_editor→execution_log→batch_tracker", ok=True)
    vault = DirtyDataVault()

    # 1) 流程定义
    bots = store.rows("bot_definition")
    if bot_id is not None:
        bots = [b for b in bots if b.get("id") == bot_id]
    if not bots:
        result.add_error("无流程定义数据")
        result.ok = False
        return result
    bot = bots[0]
    result.add_step("bot_definition", bot)
    result.data["bot"] = bot["流程名称"]

    # 2) 步骤编排：只消费显式归属该流程的步骤，未归属草稿不进入链路
    steps = step_ownership.owned_steps(int(bot["id"]))
    if not steps:
        result.add_error("该流程没有显式归属的步骤编排")
        result.ok = False
        return result
    for step in steps:
        result.add_step("step_editor", step)
        if step.get("status") not in ("已编排", "已验证"):
            vault.isolate(step, f"步骤状态异常: {step.get('status')}", "step_editor")
    result.data["steps"] = [s["步骤名称"] for s in steps]
    result.data["step_keys"] = [s[step_ownership.KEY_FIELD] for s in steps]
    result.data["step_count"] = len(steps)

    # 3) 版本快照：发布后的版本只引用已归属步骤
    snapshot = store.flow_versions.latest_snapshot(int(bot["id"]))
    result.data["version"] = snapshot["version"] if snapshot else None
    result.data["snapshot_steps"] = len(snapshot["step_ids"]) if snapshot else 0
    result.data["unassigned_steps"] = len(step_ownership.unowned_steps())

    # 4) 执行记录
    execs = store.rows("execution_log")
    if not execs:
        result.add_error("无执行记录")
        result.ok = False
        return result
    for ex in execs:
        result.add_step("execution_log", ex)
        if ex.get("status") == "执行失败":
            vault.isolate(ex, f"执行失败: {ex.get('错误摘要','')}", "execution_log")
    result.data["exec_ok"] = sum(1 for e in execs if e.get("status") == "执行成功")
    result.data["exec_fail"] = sum(1 for e in execs if e.get("status") == "执行失败")

    # 5) 批次追踪
    batches = store.rows("batch_tracker")
    if not batches:
        result.add_error("无批次追踪数据")
        result.ok = False
        return result
    for bt in batches:
        result.add_step("batch_tracker", bt)
        if bt.get("status") == "有遗漏":
            vault.isolate(bt, "批次存在遗漏", "batch_tracker")
    result.data["batch_count"] = len(batches)
    result.data["batch_complete"] = sum(1 for b in batches if b.get("status") == "已完成")

    result.dirty = vault.list_all()
    return result


# ---------------------------------------------------------------------------
# 链路 2: trigger_config → scheduler → task_queue
# ---------------------------------------------------------------------------

def pipeline_trigger_to_queue(trigger_id: int | None = None) -> PipelineResult:
    """触发配置 → 调度计划 → 任务队列 排队执行链路。"""
    result = PipelineResult(pipeline="trigger_config→scheduler→task_queue", ok=True)
    vault = DirtyDataVault()

    # 1) 触发配置
    triggers = store.rows("trigger_config")
    if trigger_id is not None:
        triggers = [t for t in triggers if t.get("id") == trigger_id]
    if not triggers:
        result.add_error("无触发配置")
        result.ok = False
        return result
    tg = triggers[0]
    result.add_step("trigger_config", tg)
    result.data["trigger"] = tg.get("触发器名称")
    result.data["trigger_type"] = tg.get("触发类型")

    # 2) 调度计划（消费触发配置）
    plans = store.rows("scheduler")
    if not plans:
        result.add_error("无调度计划")
        result.ok = False
        return result
    for plan in plans:
        result.add_step("scheduler", plan)
        if plan.get("status") not in ("已激活",):
            vault.isolate(plan, f"调度计划未激活: {plan.get('status')}", "scheduler")
    result.data["plan_count"] = len(plans)

    # 3) 任务队列
    tasks = store.rows("task_queue")
    if not tasks:
        result.add_error("无任务队列")
        result.ok = False
        return result
    for tq in tasks:
        result.add_step("task_queue", tq)
        if tq.get("status") == "执行失败":
            vault.isolate(tq, "任务执行失败", "task_queue")
    result.data["queue_waiting"] = sum(1 for t in tasks if t.get("status") == "排队中")
    result.data["queue_done"] = sum(1 for t in tasks if t.get("status") == "已完成")

    result.dirty = vault.list_all()
    return result


# ---------------------------------------------------------------------------
# 链路 3: element_selector → page_model → data_extract → result_verify → output_format
# ---------------------------------------------------------------------------

def pipeline_element_to_output(elem_id: int | None = None) -> PipelineResult:
    """元素定位 → 页面建模 → 数据提取 → 结果校验 → 输出格式 完整采集链路。"""
    result = PipelineResult(pipeline="element_selector→page_model→data_extract→result_verify→output_format", ok=True)
    vault = DirtyDataVault()

    # 1) 元素定位
    elems = store.rows("element_selector")
    if elem_id is not None:
        elems = [e for e in elems if e.get("id") == elem_id]
    if not elems:
        result.add_error("无元素定位")
        result.ok = False
        return result
    el = elems[0]
    result.add_step("element_selector", el)
    result.data["selector"] = el["规则名称"]

    # 2) 页面建模
    pages = store.rows("page_model")
    if not pages:
        result.add_error("无页面模型")
        result.ok = False
        return result
    for pg in pages:
        result.add_step("page_model", pg)
        if pg.get("status") == "结构失效":
            vault.isolate(pg, "页面结构已失效", "page_model")
    result.data["page_count"] = len(pages)

    # 3) 数据提取
    extracts = store.rows("data_extract")
    if not extracts:
        result.add_error("无数据提取")
        result.ok = False
        return result
    for ext in extracts:
        result.add_step("data_extract", ext)
        if ext.get("status") not in ("已测试", "已生效"):
            vault.isolate(ext, f"提取规则未生效: {ext.get('status')}", "data_extract")
    result.data["extract_count"] = len(extracts)

    # 4) 结果校验
    verifies = store.rows("result_verify")
    if not verifies:
        result.add_error("无结果校验")
        result.ok = False
        return result
    for vf in verifies:
        result.add_step("result_verify", vf)
        if vf.get("status") != "已启用":
            vault.isolate(vf, f"校验规则未启用: {vf.get('status')}", "result_verify")
    result.data["verify_active"] = sum(1 for v in verifies if v.get("status") == "已启用")

    # 5) 输出格式
    outputs = store.rows("output_format")
    if not outputs:
        result.add_error("无输出格式")
        result.ok = False
        return result
    for out in outputs:
        result.add_step("output_format", out)
    result.data["output_count"] = len(outputs)

    result.dirty = vault.list_all()
    return result


# ---------------------------------------------------------------------------
# 链路 4: proxy_router → rate_limiter → anti_block 联动
# ---------------------------------------------------------------------------

def pipeline_proxy_anti_block(proxy_id: int | None = None) -> PipelineResult:
    """代理路由 → 频控策略 → 反封锁 联动链路。"""
    result = PipelineResult(pipeline="proxy_router→rate_limiter→anti_block", ok=True)
    vault = DirtyDataVault()

    # 1) 代理路由
    proxies = store.rows("proxy_router")
    if proxy_id is not None:
        proxies = [p for p in proxies if p.get("id") == proxy_id]
    if not proxies:
        result.add_error("无代理节点")
        result.ok = False
        return result
    online = [p for p in proxies if p.get("status") == "在线"]
    result.add_step("proxy_router", proxies[0])
    result.data["proxy_online"] = len(online)
    result.data["proxy_total"] = len(proxies)
    if not online:
        result.add_error("无在线代理节点，无法继续")
        result.ok = False
        return result

    # 2) 频控策略
    raters = store.rows("rate_limiter")
    if not raters:
        result.add_error("无频控策略")
        result.ok = False
        return result
    for rl in raters:
        result.add_step("rate_limiter", rl)
        if rl.get("status") == "已超限":
            vault.isolate(rl, "频控已超限", "rate_limiter")
            result.wait_required = True
    result.data["rater_active"] = sum(1 for r in raters if r.get("status") == "已生效")

    # 3) 反封锁
    blocks = store.rows("anti_block")
    if not blocks:
        result.add_error("无反封锁策略")
        result.ok = False
        return result
    for ab in blocks:
        result.add_step("anti_block", ab)
        if ab.get("status") == "触发封锁":
            vault.isolate(ab, "已触发封锁", "anti_block")
            if result.data["proxy_online"] > 1:
                result.data["proxy_switch_needed"] = True
    result.data["block_active"] = sum(1 for b in blocks if b.get("status") == "已启用")
    result.data["block_triggered"] = sum(1 for b in blocks if b.get("status") == "触发封锁")

    result.dirty = vault.list_all()
    return result


# ---------------------------------------------------------------------------
# 链路 5: data_enrich → result_verify 二次验证
# ---------------------------------------------------------------------------

def pipeline_enrich_verify(enrich_id: int | None = None) -> PipelineResult:
    """数据补全 → 结果校验 二次验证链路。"""
    result = PipelineResult(pipeline="data_enrich→result_verify", ok=True)
    vault = DirtyDataVault()

    # 1) 数据补全
    enriches = store.rows("data_enrich")
    if enrich_id is not None:
        enriches = [e for e in enriches if e.get("id") == enrich_id]
    if not enriches:
        result.add_error("无数据补全规则")
        result.ok = False
        return result
    for ent in enriches:
        result.add_step("data_enrich", ent)
        if ent.get("status") != "已生效":
            vault.isolate(ent, f"补全规则未生效: {ent.get('status')}", "data_enrich")
    result.data["enrich_active"] = sum(1 for e in enriches if e.get("status") == "已生效")

    # 2) 结果校验（二次验证）
    verifies = store.rows("result_verify")
    if not verifies:
        result.add_error("无校验规则，二次验证无法执行")
        result.ok = False
        return result
    for vf in verifies:
        result.add_step("result_verify", vf)
        if vf.get("status") != "已启用":
            vault.isolate(vf, f"二次验证规则未启用: {vf.get('status')}", "result_verify")
    result.data["reverify_rules"] = len(verifies)
    result.data["reverify_done"] = True

    result.dirty = vault.list_all()
    return result


# ---------------------------------------------------------------------------
# 链路 6: credential_store → env_profile 绑定
# ---------------------------------------------------------------------------

def pipeline_credential_env(cred_id: int | None = None) -> PipelineResult:
    """凭据管理 → 环境配置 绑定执行环境链路。"""
    result = PipelineResult(pipeline="credential_store→env_profile", ok=True)
    vault = DirtyDataVault()

    # 1) 凭据管理
    creds = store.rows("credential_store")
    if cred_id is not None:
        creds = [c for c in creds if c.get("id") == cred_id]
    if not creds:
        result.add_error("无凭据数据")
        result.ok = False
        return result
    for cr in creds:
        result.add_step("credential_store", cr)
        if cr.get("status") in ("已过期", "已吊销"):
            vault.isolate(cr, f"凭据不可用: {cr.get('status')}", "credential_store")
    result.data["cred_normal"] = sum(1 for c in creds if c.get("status") == "正常")

    # 2) 环境配置
    envs = store.rows("env_profile")
    if not envs:
        result.add_error("无环境配置")
        result.ok = False
        return result
    for env in envs:
        result.add_step("env_profile", env)
        if env.get("status") not in ("可用",):
            vault.isolate(env, f"环境不可用: {env.get('status')}", "env_profile")
    result.data["env_available"] = sum(1 for e in envs if e.get("status") == "可用")
    result.data["bound"] = result.data["cred_normal"] > 0 and result.data["env_available"] > 0

    result.dirty = vault.list_all()
    return result


# ---------------------------------------------------------------------------
# 链路 7: anomaly_log → execution_log 关联追踪
# ---------------------------------------------------------------------------

def pipeline_anomaly_to_exec(anomaly_id: int | None = None) -> PipelineResult:
    """异常日志 → 执行记录 关联追踪链路。"""
    result = PipelineResult(pipeline="anomaly_log→execution_log", ok=True)
    vault = DirtyDataVault()

    # 1) 异常日志
    anoms = store.rows("anomaly_log")
    if anomaly_id is not None:
        anoms = [a for a in anoms if a.get("id") == anomaly_id]
    if not anoms:
        result.add_error("无异常日志")
        result.ok = False
        return result
    for an in anoms:
        result.add_step("anomaly_log", an)
        if an.get("status") in ("未处理", "处理中"):
            vault.isolate(an, f"异常未恢复: {an.get('status')}", "anomaly_log")
    result.data["anom_unresolved"] = sum(1 for a in anoms if a.get("status") in ("未处理", "处理中"))
    result.data["anom_resolved"] = sum(1 for a in anoms if a.get("status") == "已恢复")

    # 2) 关联执行记录
    execs = store.rows("execution_log")
    if not execs:
        result.add_error("无执行记录关联")
        result.ok = False
        return result
    for ex in execs:
        result.add_step("execution_log", ex)
        if ex.get("status") == "执行失败":
            result.data["related_failure"] = ex.get("执行编号", "?")
    if result.data["anom_unresolved"] > 0 and any(
        e.get("status") == "执行失败" for e in execs
    ):
        result.data["anomaly_linked"] = True

    result.dirty = vault.list_all()
    return result


# ---------------------------------------------------------------------------
# 批次处理：遗漏检测和补采触发
# ---------------------------------------------------------------------------

def detect_missed_batches() -> list[dict[str, Any]]:
    """检测存在遗漏的批次并返回补采建议。"""
    batches = store.rows("batch_tracker")
    missed: list[dict[str, Any]] = []
    for bt in batches:
        status = bt.get("status", "")
        if status == "有遗漏":
            missed.append({
                "batch_id": bt.get("id"),
                "batch_no": bt.get("批次编号", "?"),
                "target": bt.get("目标条数", "?"),
                "success": bt.get("成功条数", "?"),
                "missed": bt.get("遗漏条数", "?"),
                "trigger_resupply": True,
            })
    return missed


def trigger_resupply(missed: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """为遗漏批次生成补采任务并写入任务队列。"""
    tasks = store.rows("task_queue")
    created: list[dict[str, Any]] = []
    for batch_info in missed:
        new_id = max((int(t.get("id", 0)) for t in tasks), default=0) + 1
        task = {
            "id": new_id,
            "任务编号": f"RESU-{1000 + new_id}",
            "流程名称": f"补采-批次{batch_info['batch_no']}",
            "提交时间": "2026-11-17",
            "优先级": "高",
            "预计执行": "立即",
            "排队时长": "0min",
            "当前状态": "排队中",
            "status": "排队中",
            "pending": True,
            "abnormal": False,
        }
        tasks.append(task)
        created.append(task)
    return created


def pipeline_batch_resupply() -> dict[str, Any]:
    """批次遗漏 → 补采触发的完整处理。"""
    missed = detect_missed_batches()
    if not missed:
        return {"missed_count": 0, "resupply_tasks": [], "ok": True}
    tasks = trigger_resupply(missed)
    return {
        "missed_count": len(missed),
        "resupply_tasks": len(tasks),
        "ok": len(tasks) > 0,
        "details": missed,
    }


# ---------------------------------------------------------------------------
# 统一入口
# ---------------------------------------------------------------------------

def run_all_pipelines() -> dict[str, Any]:
    """运行全部贯通链路并返回综合报告。"""
    recovery = RecoveryManager(max_retries=3)

    pipelines = {
        "lifecycle": lambda: pipeline_rpa_lifecycle(),
        "trigger_queue": lambda: pipeline_trigger_to_queue(),
        "element_output": lambda: pipeline_element_to_output(),
        "proxy_anti_block": lambda: pipeline_proxy_anti_block(),
        "enrich_verify": lambda: pipeline_enrich_verify(),
        "credential_env": lambda: pipeline_credential_env(),
        "anomaly_exec": lambda: pipeline_anomaly_to_exec(),
    }

    report: dict[str, Any] = {}
    all_ok = True

    for name, fn in pipelines.items():
        ok, result, err = recovery.attempt(f"pipeline_{name}", fn)
        if not ok:
            report[name] = {"ok": False, "error": err}
            all_ok = False
        else:
            report[name] = result.summary
            if not result.ok:
                all_ok = False

    # 批次补采
    resupply = pipeline_batch_resupply()
    report["batch_resupply"] = resupply

    report["all_ok"] = all_ok
    report["dirty_vault_size"] = DirtyDataVault().size()
    report["recovery_history"] = recovery.history()

    return report