<script setup>
import { onMounted, ref } from 'vue'
import { getJSON } from '../api'
const rows = ref([])
const DISPO = {
  applied: { label: '单条成功', cls: 'ok' },
  txn_applied: { label: '整批提交', cls: 'ok' },
  txn_rolled_back: { label: '整批回滚', cls: 'danger' },
  skipped_duplicate: { label: '重复跳过', cls: 'warn' },
  quarantine_gap: { label: '缺口隔离', cls: 'warn' },
  apply_error: { label: '应用错误', cls: 'danger' },
}
function dispoOf(r) {
  return DISPO[r.disposition] || { label: r.disposition, cls: 'warn' }
}
onMounted(async () => { rows.value = await getJSON('/api/ledger') })
</script>
<template>
  <div class="page">
    <h1>变更账本</h1>
    <p class="muted">每次 Apply 写入的事件处置记录（与报告同源）；txn_id 非空即事务批次事件。</p>
    <div class="panel">
      <table>
        <thead>
          <tr><th>ID</th><th>流</th><th>run</th><th>seq</th><th>op</th><th>entity</th><th>txn_id</th><th>disposition</th><th>event_ts</th></tr>
        </thead>
        <tbody>
          <tr v-for="r in rows" :key="r.id">
            <td>{{ r.id }}</td>
            <td>{{ r.stream_id }}</td>
            <td>{{ r.run_id }}</td>
            <td>{{ r.seq }}</td>
            <td>{{ r.op }}</td>
            <td>{{ r.entity_key }}</td>
            <td>{{ r.txn_id || '—' }}</td>
            <td><span class="tag" :class="dispoOf(r).cls">{{ dispoOf(r).label }}</span></td>
            <td>{{ r.event_ts }}</td>
          </tr>
        </tbody>
      </table>
    </div>
  </div>
</template>
