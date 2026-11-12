<script setup>
import { onMounted, ref } from 'vue'
import { getJSON, postJSON } from '../api'
const streams = ref([])
const streamId = ref(1)
const last = ref(null)
const runs = ref([])
const batches = ref([])
const err = ref('')
const busy = ref(false)
async function refreshRuns() {
  runs.value = await getJSON('/api/runs')
}
async function refreshBatches() {
  batches.value = await getJSON('/api/txn_batches')
}
async function runApply() {
  busy.value = true
  err.value = ''
  try {
    last.value = await postJSON('/api/apply', { stream_id: streamId.value })
    streams.value = await getJSON('/api/streams')
    await refreshRuns()
    await refreshBatches()
  } catch (e) {
    err.value = String(e.message || e)
  } finally {
    busy.value = false
  }
}
function txnOf(run) {
  return run && run.report && run.report.txn ? run.report.txn : null
}
function batchTagClass(status) {
  if (status === 'committed') return 'ok'
  if (status === 'rolled_back') return 'danger'
  return 'warn'
}
onMounted(async () => {
  streams.value = await getJSON('/api/streams')
  if (streams.value.length) streamId.value = streams.value[0].id
  await refreshRuns()
  await refreshBatches()
})
</script>
<template>
  <div class="page">
    <h1>应用台</h1>
    <p class="muted">按游标连续序应用；同事务（txn_id）事件整批提交，任一条失败则整批回滚、游标停在批前。</p>
    <div class="row-actions panel">
      <label>流
        <select v-model.number="streamId">
          <option v-for="s in streams" :key="s.id" :value="s.id">{{ s.id }} · {{ s.name }}（游标 {{ s.apply_cursor }}）</option>
        </select>
      </label>
      <button :disabled="busy" @click="runApply">{{ busy ? '应用中…' : '执行 Apply' }}</button>
    </div>
    <p v-if="err" class="tag danger">{{ err }}</p>
    <div class="panel" v-if="last">
      <h3>最近一次</h3>
      <p>
        run #{{ last.id }} ·
        <span class="tag" :class="last.status === 'succeeded' ? 'ok' : 'warn'">{{ last.status }}</span>
        · loaded {{ last.loaded_count }} / applied {{ last.applied_count }} /
        quarantined {{ last.quarantined_count }} / skipped {{ last.skipped_count }}
        · cursor {{ last.cursor_before }} → {{ last.cursor_after }}
      </p>
      <p v-if="txnOf(last)">
        事务批次 {{ txnOf(last).batch_count }} ·
        <span class="tag ok">整批成功 {{ txnOf(last).committed }}</span>
        <span class="tag danger">整批回滚 {{ txnOf(last).rolled_back }}</span>
        <span v-if="txnOf(last).skipped" class="tag warn">重复跳过 {{ txnOf(last).skipped }}</span>
        · 涉及事件 {{ txnOf(last).events }}
      </p>
      <table v-if="txnOf(last) && txnOf(last).batches.length">
        <thead>
          <tr><th>txn_id</th><th>seq 范围</th><th>事件数</th><th>结果</th><th>回滚原因</th></tr>
        </thead>
        <tbody>
          <tr v-for="b in txnOf(last).batches" :key="b.txn_id + ':' + b.seq_start">
            <td>{{ b.txn_id }}</td>
            <td>{{ b.seq_start }}→{{ b.seq_end }}</td>
            <td>{{ b.event_count }}</td>
            <td><span class="tag" :class="batchTagClass(b.status)">{{ b.status === 'committed' ? '整批提交' : b.status === 'rolled_back' ? '整批回滚' : '重复跳过' }}</span></td>
            <td>{{ b.reason || '—' }}</td>
          </tr>
        </tbody>
      </table>
      <pre class="mono">{{ JSON.stringify(last.report, null, 2) }}</pre>
    </div>
    <div class="panel">
      <h3>历史 Runs</h3>
      <table>
        <thead>
          <tr><th>ID</th><th>流</th><th>状态</th><th>loaded</th><th>applied</th><th>quarantine</th><th>skipped</th><th>cursor</th><th>事务批次(✓/✗)</th><th>时间</th></tr>
        </thead>
        <tbody>
          <tr v-for="r in runs" :key="r.id">
            <td>{{ r.id }}</td>
            <td>{{ r.stream_id }}</td>
            <td>{{ r.status }}</td>
            <td>{{ r.loaded_count }}</td>
            <td>{{ r.applied_count }}</td>
            <td>{{ r.quarantined_count }}</td>
            <td>{{ r.skipped_count }}</td>
            <td>{{ r.cursor_before }}→{{ r.cursor_after }}</td>
            <td>{{ txnOf(r) ? `${txnOf(r).committed} / ${txnOf(r).rolled_back}（共 ${txnOf(r).batch_count}）` : '—' }}</td>
            <td>{{ r.created_at }}</td>
          </tr>
        </tbody>
      </table>
    </div>
    <div class="panel">
      <h3>事务批次记录</h3>
      <p class="muted">持久化的批次结果（txn_batches 表），刷新后仍在，与账本 / 隔离区按 run_id + seq 对账。</p>
      <table>
        <thead>
          <tr><th>流</th><th>run</th><th>txn_id</th><th>seq 范围</th><th>事件数</th><th>结果</th><th>原因</th><th>时间</th></tr>
        </thead>
        <tbody>
          <tr v-for="b in batches" :key="b.id">
            <td>{{ b.stream_id }}</td>
            <td>{{ b.run_id }}</td>
            <td>{{ b.txn_id }}</td>
            <td>{{ b.seq_start }}→{{ b.seq_end }}</td>
            <td>{{ b.event_count }}</td>
            <td><span class="tag" :class="batchTagClass(b.status)">{{ b.status === 'committed' ? '整批提交' : b.status === 'rolled_back' ? '整批回滚' : '重复跳过' }}</span></td>
            <td>{{ b.reason || '—' }}</td>
            <td>{{ b.created_at }}</td>
          </tr>
          <tr v-if="!batches.length"><td colspan="8" class="muted">暂无事务批次记录</td></tr>
        </tbody>
      </table>
    </div>
  </div>
</template>
