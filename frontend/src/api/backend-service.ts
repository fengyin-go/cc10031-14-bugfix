import { request } from './client'
import { moduleMeta } from '@/data/modules'
import type { ActionResult, EntryRow, OverviewResult, PageResult } from '@/data/types'

export { moduleMeta }

export async function listEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<PageResult> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}?${query}`)
  if (!response.ok) throw new Error('业务数据读取失败')
  return await response.json()
}

export async function createEntry(
  key: string,
  values: Record<string, string>,
): Promise<ActionResult> {
  const response = await request(`/api/${key}`, {
    method: 'POST',
    body: JSON.stringify({ values }),
  })
  return await response.json()
}

export async function runAction(
  key: string,
  id: number,
  action: string,
): Promise<ActionResult> {
  const response = await request(`/api/${key}/${id}/actions`, {
    method: 'POST',
    body: JSON.stringify({ action }),
  })
  return await response.json()
}

export async function downloadEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<void> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}/export?${query}`)
  if (!response.ok) throw new Error('业务数据导出失败')
  const url = URL.createObjectURL(await response.blob())
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = `${key}.csv`
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
  URL.revokeObjectURL(url)
}

export async function loadOverview(): Promise<OverviewResult> {
  const response = await request('/api/overview')
  if (!response.ok) throw new Error('运营概览读取失败')
  return await response.json()
}

/** 跨模块数据流：获取全平台领域快照 */
export async function loadNodeStats(module?: string): Promise<Record<string, unknown>> {
  const query = module ? `?module=${module}` : ''
  const response = await request(`/api/nodestats${query}`)
  if (!response.ok) throw new Error('节点统计读取失败')
  return await response.json()
}

/** 跨模块数据流：获取指定 topic 的通道消息 */
export async function loadChannelSnapshot(topic: string): Promise<Record<string, unknown>> {
  const response = await request(`/api/channel/${topic}`)
  if (!response.ok) throw new Error('通道快照读取失败')
  return await response.json()
}

/** 跨模块数据流：查询脏数据隔离区 */
export async function loadDirtyVault(source?: string): Promise<Record<string, unknown>> {
  const query = source ? `?source=${source}` : ''
  const response = await request(`/api/dirty-vault${query}`)
  if (!response.ok) throw new Error('脏数据查询失败')
  return await response.json()
}

/** 跨模块数据流：查询采集记录 */
export async function loadCollectRecords(batchRef?: string): Promise<Record<string, unknown>> {
  const query = batchRef ? `?batch_ref=${batchRef}` : ''
  const response = await request(`/api/collect-records${query}`)
  if (!response.ok) throw new Error('采集记录查询失败')
  return await response.json()
}

/** 跨模块数据流：执行RPA全生命周期管道 */
export async function triggerPipeline(lifecycle: string): Promise<Record<string, unknown>> {
  const response = await request(`/api/pipeline/${lifecycle}`, {
    method: 'POST',
  })
  if (!response.ok) throw new Error('管道执行失败')
  return await response.json()
}

/** 跨模块数据流：获取失败恢复状态 */
export async function loadRecoveryStats(): Promise<Record<string, unknown>> {
  const response = await request('/api/recovery')
  if (!response.ok) throw new Error('恢复状态查询失败')
  return await response.json()
}

// ---- 补全工作台 / 补全二次校验（14 号处理） ----

export type EnrichWorkbenchItem = {
  entry_id: number
  batch_ref: string
  enrich_version: number
  enriched_at: string | null
  rewritten_fields: string[]
  rewrite_diff: Record<string, { old: unknown; new: unknown }>
  validation_status: string
  validation_version: number
  validated: boolean
  quality_score: number | null
  stale_receipt: boolean
}

export type EnrichWorkbench = {
  batch_ref: string | null
  item_no: number
  item: string
  count: number
  pending_revalidation: number
  items: EnrichWorkbenchItem[]
}

export type RevalidationRecord = {
  entry_id: number
  enrich_version: number
  rewritten_fields: string[]
  validation_status: string
  validated: boolean
  quality_score: number | null
  failed_rules: string[]
}

export type RevalidationReport = {
  batch_ref: string
  item_no: number
  item: string
  terminal_state: string
  failed_rule_hits: Record<string, number>
  records: RevalidationRecord[]
  consistency: { consistent: boolean; stale_conclusions: unknown[] }
}

export type RevalidationResult = {
  ok: boolean
  batch_ref: string
  item_no: number
  item: string
  mode: string
  terminal_state: string
  revalidated: number
  passed: number
  failed: number
  conflicts: number
  consistency: { consistent: boolean; stale_conclusions: unknown[] }
  message?: string
}

/** 按批次检索补全后的字段改写、校验状态与补全版本 */
export async function loadEnrichWorkbench(batchRef: string): Promise<EnrichWorkbench> {
  const query = batchRef ? `?batch_ref=${encodeURIComponent(batchRef)}` : ''
  const response = await request(`/api/enrich/workbench${query}`)
  if (!response.ok) throw new Error('补全工作台读取失败')
  return await response.json()
}

/** 执行补全（字段改写与校验失效同步发生） */
export async function runEnrich(batchRef: string): Promise<Record<string, unknown>> {
  const response = await request(
    `/api/enrich/execute?batch_ref=${encodeURIComponent(batchRef)}`,
    { method: 'POST' },
  )
  const payload = await response.json()
  if (!response.ok) throw new Error('补全执行失败')
  return payload
}

/** 补全二次校验：incremental 只重算被改写记录，full_batch 整批重校验 */
export async function runRevalidation(
  batchRef: string,
  mode: 'incremental' | 'full_batch',
): Promise<RevalidationResult> {
  const response = await request(
    `/api/enrich/revalidate?batch_ref=${encodeURIComponent(batchRef)}&mode=${mode}`,
    { method: 'POST' },
  )
  const payload = await response.json()
  if (!response.ok) throw new Error('补全二次校验失败')
  return payload
}

/** 结果校验工作台：定位具体失败规则 */
export async function loadRevalidationReport(
  batchRef: string,
): Promise<RevalidationReport> {
  const response = await request(
    `/api/enrich/revalidation-report?batch_ref=${encodeURIComponent(batchRef)}`,
  )
  if (!response.ok) throw new Error('结果校验台账读取失败')
  return await response.json()
}
