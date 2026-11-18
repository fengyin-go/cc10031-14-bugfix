<template>
  <section class="enrich-panel" :data-module="moduleKey" data-testid="enrich-panel">
    <header class="panel-head">
      <div>
        <h3>{{ isEnrich ? '补全工作台' : '结果校验台账' }}</h3>
        <p class="page-desc">
          按批次检索被改写字段、当前校验状态与补全版本；字段一旦改写，补全前的通过结论立即失效。
        </p>
      </div>
      <span class="panel-badge">14 号处理 · 补全二次校验</span>
    </header>

    <form class="filter-bar" @submit.prevent="load">
      <label class="filter-item">
        <span>批次编号</span>
        <input v-model="batchRef" data-testid="enrich-batch" placeholder="如 BATC-0001" />
      </label>
      <button class="btn" data-testid="enrich-query" type="submit">查询</button>
      <button
        v-if="isEnrich"
        class="btn primary"
        data-testid="enrich-execute"
        type="button"
        :disabled="busy || !batchRef.trim()"
        @click="executeEnrich"
      >
        执行补全
      </button>
      <button
        class="btn"
        data-testid="revalidate-incremental"
        type="button"
        :disabled="busy || !batchRef.trim()"
        @click="revalidate('incremental')"
      >
        增量重验
      </button>
      <button
        class="btn"
        data-testid="revalidate-full"
        type="button"
        :disabled="busy || !batchRef.trim()"
        @click="revalidate('full_batch')"
      >
        整批重验
      </button>
    </form>

    <div class="stat-row">
      <article class="stat-card">
        <span class="stat-label">记录数</span>
        <strong class="stat-value" data-testid="enrich-count">{{ items.length }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">待复验</span>
        <strong class="stat-value" data-testid="enrich-pending">{{ pendingCount }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">复验终态</span>
        <strong class="stat-value" data-testid="revalidate-terminal">{{ terminalState }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">旧结论一致性</span>
        <strong class="stat-value" :class="{ 'error-text': !consistent }">
          {{ consistent ? '一致' : '存在旧字段配新校验分' }}
        </strong>
      </article>
    </div>

    <table class="data-table">
      <thead>
        <tr>
          <th>记录ID</th>
          <th>补全版本</th>
          <th>被改写字段</th>
          <th>校验状态</th>
          <th>质量分</th>
          <th>失败规则</th>
        </tr>
      </thead>
      <tbody>
        <tr
          v-for="item in items"
          :key="item.entry_id"
          :data-testid="`enrich-row-${item.entry_id}`"
          :data-version="item.enrich_version"
          :data-status="item.validation_status"
        >
          <td>{{ item.entry_id }}</td>
          <td>v{{ item.enrich_version }}</td>
          <td>
            <span v-if="!item.rewritten_fields.length">—</span>
            <span
              v-for="field in item.rewritten_fields"
              :key="field"
              class="field-chip"
              :title="diffText(item, field)"
            >
              {{ field }}
            </span>
          </td>
          <td>{{ item.validation_status }}</td>
          <td>{{ item.quality_score === null ? '—' : item.quality_score }}</td>
          <td>
            <span v-if="!item.failed_rules?.length">—</span>
            <span v-for="rule in item.failed_rules ?? []" :key="rule" class="rule-chip">
              {{ rule }}
            </span>
          </td>
        </tr>
        <tr v-if="!items.length">
          <td colspan="6" class="empty-state">暂无数据，请按批次检索或先执行补全</td>
        </tr>
      </tbody>
    </table>

    <div v-if="Object.keys(failedRuleHits).length" class="failed-rules" data-testid="failed-rules">
      <strong>失败规则命中：</strong>
      <span v-for="(count, rule) in failedRuleHits" :key="rule" class="rule-chip">
        {{ rule }} × {{ count }}
      </span>
    </div>

    <footer class="page-foot">
      <span v-if="message">{{ message }}</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import {
  loadEnrichWorkbench,
  loadRevalidationReport,
  runEnrich,
  runRevalidation,
} from '@/api/backend-service'
import type { EnrichWorkbenchItem } from '@/api/backend-service'

const props = defineProps<{ moduleKey: string }>()
const isEnrich = computed(() => props.moduleKey === 'data_enrich')

const batchRef = ref('')
const items = ref<(EnrichWorkbenchItem & { failed_rules?: string[] })[]>([])
const failedRuleHits = ref<Record<string, number>>({})
const terminalState = ref('待复验')
const consistent = ref(true)
const busy = ref(false)
const message = ref('')
const errorMessage = ref('')

const pendingCount = computed(
  () => items.value.filter((item) => item.validation_status === '待复验').length,
)

function diffText(item: EnrichWorkbenchItem, field: string): string {
  const change = item.rewrite_diff?.[field]
  if (!change) return field
  return `${field}: ${String(change.old ?? '空')} → ${String(change.new ?? '空')}`
}

async function load() {
  if (!batchRef.value.trim()) return
  busy.value = true
  errorMessage.value = ''
  message.value = ''
  try {
    const workbench = await loadEnrichWorkbench(batchRef.value.trim())
    const report = await loadRevalidationReport(batchRef.value.trim())
    const failedByEntry = new Map(
      report.records.map((record) => [record.entry_id, record.failed_rules]),
    )
    items.value = workbench.items.map((item) => ({
      ...item,
      failed_rules: failedByEntry.get(item.entry_id) ?? [],
    }))
    failedRuleHits.value = report.failed_rule_hits ?? {}
    terminalState.value = report.terminal_state
    consistent.value = report.consistency?.consistent !== false
    message.value = `已载入 ${workbench.count} 条补全后明细`
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '补全数据读取失败'
  } finally {
    busy.value = false
  }
}

async function executeEnrich() {
  busy.value = true
  errorMessage.value = ''
  message.value = ''
  try {
    const result = await runEnrich(batchRef.value.trim())
    message.value = `补全改写 ${result.rewritten ?? 0} 条，旧校验状态已同步失效`
    await load()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '补全执行失败'
  } finally {
    busy.value = false
  }
}

async function revalidate(mode: 'incremental' | 'full_batch') {
  busy.value = true
  errorMessage.value = ''
  message.value = ''
  try {
    const result = await runRevalidation(batchRef.value.trim(), mode)
    message.value = `${mode === 'incremental' ? '增量' : '整批'}重验：${
      result.terminal_state
    }（重算 ${result.revalidated} 条，通过 ${result.passed}，失败 ${result.failed}，冲突 ${result.conflicts}）`
    await load()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '补全二次校验失败'
  } finally {
    busy.value = false
  }
}

onMounted(() => {
  // 从地址栏带批次进入时直接检索，省一次手动输入。
  const fromQuery = new URLSearchParams(window.location.search).get('batch_ref')
  if (fromQuery) {
    batchRef.value = fromQuery
    void load()
  }
})
</script>

<style scoped>
.enrich-panel {
  border: 1px solid var(--border);
  border-radius: 10px;
  background: #fff;
  padding: 14px 16px;
  margin: 16px 0;
}
.panel-head {
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  gap: 12px;
}
.panel-head h3 {
  margin: 0;
  font-size: 15px;
}
.panel-badge {
  font-size: 12px;
  color: var(--muted);
  border: 1px solid var(--border);
  border-radius: 999px;
  padding: 2px 10px;
  white-space: nowrap;
}
.field-chip,
.rule-chip {
  display: inline-block;
  margin: 0 4px 2px 0;
  padding: 1px 8px;
  border-radius: 999px;
  font-size: 12px;
  background: #eef4ff;
  color: #1f4fbf;
}
.rule-chip {
  background: #fdecec;
  color: #b42318;
}
.failed-rules {
  margin-top: 10px;
  font-size: 13px;
  color: var(--muted);
}
</style>
