<template>
  <div class="container">
    <header>
      <h1>GG Leaderboards</h1>
      <div class="meta">
        <span>Last update: {{ lastUpdate || '—' }}</span>
        <button @click="manualRefresh" :disabled="loading">Refresh</button>
      </div>
    </header>

    <div v-if="error" class="error">{{ error }}</div>

    <div class="grid">
      <section v-for="preview in previews" :key="preview.name" class="card">
        <h2 class="table-name">{{ preview.name }}</h2>
        <div class="table-wrapper">
          <table>
            <thead>
              <tr>
                <th v-for="c in preview.columns" :key="c">{{ c }}</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="(row, idx) in preview.rows" :key="idx">
                <td v-for="(cell, cidx) in row" :key="cidx">{{ formatCell(cell) }}</td>
              </tr>
            </tbody>
          </table>
        </div>
      </section>
    </div>
  </div>
</template>

<script setup lang="ts">
import axios from 'axios';
import { onMounted, onUnmounted, ref } from 'vue';

type Preview = { name: string; columns: string[]; rows: (string | number | null)[][] };

const previews = ref<Preview[]>([]);
const lastUpdate = ref<string>('');
const loading = ref<boolean>(false);
const error = ref<string>('');
let pollHandle: number | undefined;

async function fetchLastUpdate() {
  const res = await axios.get('/api/last_update');
  return res.data?.last_update as string;
}

async function fetchPreviews() {
  loading.value = true;
  try {
    const res = await axios.get('/api/preview');
    previews.value = res.data?.previews || [];
    error.value = '';
  } catch (e: any) {
    error.value = e?.message || 'Failed to load previews';
  } finally {
    loading.value = false;
  }
}

async function syncOnce() {
  try {
    const marker = await fetchLastUpdate();
    if (marker && marker !== lastUpdate.value) {
      lastUpdate.value = marker;
      await fetchPreviews();
    }
  } catch (e: any) {
    error.value = e?.message || 'Failed to check updates';
  }
}

function manualRefresh() {
  fetchPreviews();
}

function startPolling() {
  // poll every 10s
  pollHandle = window.setInterval(syncOnce, 10000);
}

function stopPolling() {
  if (pollHandle) window.clearInterval(pollHandle);
}

function formatCell(val: any) {
  if (val === null || val === undefined) return '—';
  return String(val);
}

onMounted(async () => {
  // Initial load
  await syncOnce();
  if (previews.value.length === 0) {
    await fetchPreviews();
  }
  startPolling();
});

onUnmounted(() => {
  stopPolling();
});
</script>

<style scoped>
.container {
  max-width: 1400px;
  margin: 0 auto;
  padding: 16px;
  font-family: system-ui, -apple-system, Segoe UI, Roboto, Ubuntu, Cantarell, Noto Sans, Helvetica, Arial, "Apple Color Emoji", "Segoe UI Emoji";
}
header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  margin-bottom: 16px;
}
.meta { display: flex; align-items: center; gap: 12px; }
.error { color: #b00020; margin-bottom: 12px; }
.grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(420px, 1fr));
  gap: 16px;
}
.card { border: 1px solid #e5e7eb; border-radius: 8px; padding: 12px; background: #fff; }
.table-name { font-size: 16px; margin: 0 0 8px; }
.table-wrapper { overflow: auto; }
table { width: 100%; border-collapse: collapse; font-size: 12px; }
th, td { border: 1px solid #f0f0f0; padding: 6px 8px; text-align: left; }
thead { background: #fafafa; position: sticky; top: 0; }
tbody tr:nth-child(odd) { background: #fcfcfc; }
button { padding: 6px 10px; border-radius: 6px; border: 1px solid #e5e7eb; background: #f8f9fa; cursor: pointer; }
button:disabled { opacity: 0.6; cursor: not-allowed; }
</style>


