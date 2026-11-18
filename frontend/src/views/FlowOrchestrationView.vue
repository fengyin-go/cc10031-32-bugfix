<template>
  <section class="page" data-module="flow_orchestration">
    <header class="page-head">
      <div>
        <h2>流程版本编排</h2>
        <p class="page-desc">
          只读已归属步骤与版本快照；未归属草稿留在待认领队列，同一草稿不会出现在多个流程中。
        </p>
      </div>
      <div class="page-actions">
        <label class="filter-item">
          <span>选择流程</span>
          <select v-model.number="selectedBotId" data-testid="select-bot">
            <option v-for="group in board.groups" :key="group.bot_id" :value="group.bot_id">
              {{ group.bot_name }}（{{ group.bot_status }}）
            </option>
          </select>
        </label>
        <button class="btn" data-testid="refresh-board" type="button" @click="reload">刷新编排</button>
        <button
          class="btn primary"
          data-testid="publish-version"
          type="button"
          :disabled="busy || !activeGroup"
          @click="publish"
        >
          发布版本
        </button>
      </div>
    </header>

    <p v-if="errorMessage" class="error-text" data-testid="board-error">{{ errorMessage }}</p>
    <p v-if="flashMessage" class="ok-text" data-testid="board-flash">{{ flashMessage }}</p>

    <div class="stat-row">
      <article class="stat-card">
        <span class="stat-label">已归属步骤</span>
        <strong class="stat-value" data-testid="stat-owned">{{ board.summary.owned_steps }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">未归属草稿</span>
        <strong class="stat-value" data-testid="stat-unassigned">{{ board.summary.unassigned_steps }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">版本快照</span>
        <strong class="stat-value" data-testid="stat-snapshots">{{ board.summary.snapshots }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">归属冲突</span>
        <strong class="stat-value" data-testid="stat-conflicts">{{ board.summary.conflicts }}</strong>
      </article>
    </div>

    <div v-if="activeGroup" class="revision-grid">
      <article class="revision-panel">
        <h3>已归属步骤 · {{ activeGroup.bot_name }}</h3>
        <p class="page-desc">
          当前版本：{{ activeGroup.version || '未发布' }}
          <span v-if="activeGroup.snapshot">
            · 快照 #{{ activeGroup.snapshot.snapshot_id }}（{{ activeGroup.snapshot.terminal_state }}）
          </span>
        </p>
        <table class="data-table">
          <thead>
            <tr><th>步骤键</th><th>序号</th><th>步骤名称</th><th>状态</th><th>操作</th></tr>
          </thead>
          <tbody>
            <tr
              v-for="step in activeGroup.owned_steps"
              :key="step.step_key"
              :data-testid="`owned-step-${step.step_id}`"
            >
              <td class="mono">{{ step.step_key }}</td>
              <td>{{ step.seq }}</td>
              <td>{{ step.name }}</td>
              <td>{{ step.status }}</td>
              <td>
                <button class="link" type="button" @click="release(step.step_id)">释放归属</button>
              </td>
            </tr>
            <tr v-if="!activeGroup.owned_steps.length">
              <td colspan="5" class="empty-state">该流程还没有已归属步骤，可先在步骤编排里提交</td>
            </tr>
          </tbody>
        </table>
      </article>

      <article class="revision-panel">
        <h3>待认领队列</h3>
        <p class="page-desc">名称相似只作提示，认领后才写入唯一归属。</p>
        <table class="data-table">
          <thead>
            <tr><th>步骤键</th><th>名称</th><th>提示</th><th>操作</th></tr>
          </thead>
          <tbody>
            <tr
              v-for="candidate in activeGroup.unassigned_candidates"
              :key="candidate.step_key"
              :data-testid="`pending-claim-${candidate.step_id}`"
            >
              <td class="mono">{{ candidate.step_key }}</td>
              <td>{{ candidate.name }}</td>
              <td>{{ candidate.reason }}</td>
              <td>
                <button class="link" type="button" @click="claim(candidate.step_id)">认领</button>
              </td>
            </tr>
            <tr v-if="!activeGroup.unassigned_candidates.length">
              <td colspan="4" class="empty-state">没有名称相似的未归属步骤</td>
            </tr>
          </tbody>
        </table>

        <h3>全部未归属草稿</h3>
        <table class="data-table">
          <thead>
            <tr><th>步骤键</th><th>序号</th><th>名称</th><th>状态</th><th>操作</th></tr>
          </thead>
          <tbody>
            <tr
              v-for="step in board.unassigned_steps"
              :key="step.step_key"
              :data-testid="`unassigned-step-${step.step_id}`"
            >
              <td class="mono">{{ step.step_key }}</td>
              <td>{{ step.seq }}</td>
              <td>{{ step.name }}</td>
              <td>{{ step.status }}</td>
              <td>
                <button class="link" type="button" @click="claim(step.step_id)">认领到当前流程</button>
              </td>
            </tr>
            <tr v-if="!board.unassigned_steps.length">
              <td colspan="5" class="empty-state">没有未归属草稿</td>
            </tr>
          </tbody>
        </table>
      </article>
    </div>

    <article class="revision-panel">
      <h3>版本快照</h3>
      <table class="data-table">
        <thead>
          <tr><th>快照</th><th>流程</th><th>版本</th><th>步骤键</th><th>终态</th><th>生成时间</th></tr>
        </thead>
        <tbody>
          <tr v-for="snapshot in board.snapshots" :key="snapshot.snapshot_id" :data-testid="`snapshot-${snapshot.snapshot_id}`">
            <td>#{{ snapshot.snapshot_id }}</td>
            <td>{{ snapshot.bot_name }}</td>
            <td>{{ snapshot.version }}</td>
            <td class="mono">{{ snapshot.step_keys.join('，') || '—' }}</td>
            <td>{{ snapshot.terminal_state }}<span v-if="snapshot.terminal_code">（{{ snapshot.terminal_code }}）</span></td>
            <td>{{ snapshot.created_at }}</td>
          </tr>
          <tr v-if="!board.snapshots.length">
            <td colspan="6" class="empty-state">尚未发布任何流程版本</td>
          </tr>
        </tbody>
      </table>
    </article>

    <article class="revision-panel">
      <h3>已归属步骤执行计划</h3>
      <p class="page-desc">任务队列按稳定步骤键生成执行项，只引用版本快照中的步骤。</p>
      <table v-if="plan" class="data-table" data-testid="execution-plan">
        <thead>
          <tr><th>顺序</th><th>步骤键</th><th>步骤名称</th><th>类型</th></tr>
        </thead>
        <tbody>
          <tr v-for="step in plan.steps" :key="step.step_key">
            <td>{{ step.seq }}</td>
            <td class="mono">{{ step.step_key }}</td>
            <td>{{ step.name }}</td>
            <td>{{ step.type }}</td>
          </tr>
        </tbody>
      </table>
      <p v-else class="empty-state">当前流程尚无已发布版本，暂无可执行计划</p>
    </article>

    <article v-if="board.conflicts.length" class="revision-panel">
      <h3>归属冲突（32 号处理终态）</h3>
      <table class="data-table">
        <thead><tr><th>流程</th><th>终态码</th><th>说明</th></tr></thead>
        <tbody>
          <tr v-for="conflict in board.conflicts" :key="`${conflict.bot_id}-${conflict.terminal_code}`">
            <td>{{ conflict.bot_name }}</td>
            <td>{{ conflict.terminal_code }}</td>
            <td>{{ conflict.message }}</td>
          </tr>
        </tbody>
      </table>
    </article>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import {
  claimStep,
  loadExecutionPlan,
  loadFlowBoard,
  publishFlowVersion,
  releaseStep,
  type ExecutionPlan,
  type FlowBoard,
} from '@/api/flow-version'

const board = ref<FlowBoard>({
  groups: [],
  unassigned_steps: [],
  snapshots: [],
  conflicts: [],
  summary: { owned_steps: 0, unassigned_steps: 0, snapshots: 0, conflicts: 0 },
})
const selectedBotId = ref<number | null>(null)
const plan = ref<ExecutionPlan | null>(null)
const busy = ref(false)
const errorMessage = ref('')
const flashMessage = ref('')

const activeGroup = computed(() =>
  board.value.groups.find((group) => group.bot_id === selectedBotId.value) ?? board.value.groups[0] ?? null,
)

async function loadPlan() {
  if (!activeGroup.value) {
    plan.value = null
    return
  }
  try {
    plan.value = await loadExecutionPlan(activeGroup.value.bot_id)
  } catch {
    plan.value = null
  }
}

async function reload() {
  errorMessage.value = ''
  try {
    board.value = await loadFlowBoard()
    if (!board.value.groups.some((group) => group.bot_id === selectedBotId.value)) {
      selectedBotId.value = board.value.groups[0]?.bot_id ?? null
    }
    await loadPlan()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '编排数据读取失败'
  }
}

async function publish() {
  if (!activeGroup.value) return
  busy.value = true
  errorMessage.value = ''
  flashMessage.value = ''
  try {
    const result = await publishFlowVersion(activeGroup.value.bot_id)
    flashMessage.value = result.message
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '发布版本失败'
  } finally {
    busy.value = false
    await reload()
  }
}

async function claim(stepId: number) {
  if (!activeGroup.value) return
  busy.value = true
  errorMessage.value = ''
  flashMessage.value = ''
  try {
    const result = await claimStep(stepId, activeGroup.value.bot_id)
    flashMessage.value = result.message
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '认领失败'
  } finally {
    busy.value = false
    await reload()
  }
}

async function release(stepId: number) {
  busy.value = true
  errorMessage.value = ''
  flashMessage.value = ''
  try {
    const result = await releaseStep(stepId)
    flashMessage.value = result.message
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '释放归属失败'
  } finally {
    busy.value = false
    await reload()
  }
}

onMounted(reload)
</script>
