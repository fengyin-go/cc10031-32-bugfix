/** 流程版本与步骤归属接口：编排页读取归属/快照，发布与认领动作都走这里。 */
import { request } from './client'

export type OwnedStep = {
  step_id: number
  step_key: string
  seq: string
  name: string
  type: string
  status: string
  bot_id: number | null
  bot_name: string
  version: string
  claim_state: string
}

export type UnassignedStep = Omit<OwnedStep, 'bot_id' | 'bot_name' | 'version'>

export type PendingClaim = {
  step_id: number
  step_key: string
  name: string
  seq: string
  status: string
  suggested_bot_name: string
  reason: string
}

export type VersionSnapshot = {
  snapshot_id: number
  bot_id: number
  bot_name: string
  version: string
  step_ids: number[]
  step_keys: string[]
  terminal_state: string
  terminal_code: number | null
  created_at: string
}

export type FlowGroup = {
  bot_id: number
  bot_name: string
  bot_status: string
  version: string
  owned_steps: OwnedStep[]
  unassigned_candidates: PendingClaim[]
  snapshot: VersionSnapshot | null
}

export type FlowBoard = {
  groups: FlowGroup[]
  unassigned_steps: UnassignedStep[]
  snapshots: VersionSnapshot[]
  conflicts: { bot_id: number; bot_name: string; terminal_code: number; message: string }[]
  summary: {
    owned_steps: number
    unassigned_steps: number
    snapshots: number
    conflicts: number
  }
}

export type ExecutionPlan = {
  bot_id: number
  version: string
  snapshot_id: number
  total_steps: number
  steps: {
    seq: number
    step_id: number
    step_key: string
    name: string
    type: string
  }[]
}

async function read<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await request(path, init)
  if (!response.ok) {
    const detail = await response.json().catch(() => ({}))
    throw new Error(detail.detail ?? '流程版本接口请求失败')
  }
  return (await response.json()) as T
}

export function loadFlowBoard(botId?: number): Promise<FlowBoard> {
  const query = botId ? `?bot_id=${botId}` : ''
  return read<FlowBoard>(`/api/flow-versions${query}`)
}

export function loadExecutionPlan(botId: number, version?: string): Promise<ExecutionPlan> {
  const query = version
    ? `?bot_id=${botId}&version=${encodeURIComponent(version)}`
    : `?bot_id=${botId}`
  return read<ExecutionPlan>(`/api/flow-versions/execution-plan${query}`)
}

export function publishFlowVersion(botId: number): Promise<{ ok: boolean; message: string }> {
  return read(`/api/flow-versions/publish`, {
    method: 'POST',
    body: JSON.stringify({ bot_id: botId }),
  })
}

export function claimStep(stepId: number, botId: number): Promise<{ ok: boolean; message: string }> {
  return read(`/api/flow-versions/claim`, {
    method: 'POST',
    body: JSON.stringify({ step_id: stepId, bot_id: botId }),
  })
}

export function releaseStep(stepId: number): Promise<{ ok: boolean; message: string }> {
  return read(`/api/flow-versions/release`, {
    method: 'POST',
    body: JSON.stringify({ step_id: stepId }),
  })
}
