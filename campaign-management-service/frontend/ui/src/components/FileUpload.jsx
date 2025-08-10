import React, { useState, useCallback, useTransition, useMemo, useRef, useEffect } from 'react';
import axios from 'axios';
import {
  Button, Typography, Grid, Box, TextField, LinearProgress, Alert,
  CircularProgress, Chip, Switch, FormControlLabel,
  Dialog, DialogTitle, DialogContent, DialogActions,
  Table, TableHead, TableRow, TableCell, TableBody, Tooltip, Divider
} from '@mui/material';
import { motion } from 'framer-motion';
import { toast } from 'react-toastify';

// ── inline stand-ins (no cross-folder deps) ──
const UiCard = ({ children, sx, ...rest }) => (
  <Box sx={{ p: 2, borderRadius: 2, background: 'transparent', ...sx }} {...rest}>{children}</Box>
);
const PageContainer = ({ children }) => <Box sx={{ p: 2 }}>{children}</Box>;
// ─────────────────────────────────────────────

const cfg = {
  API_KEY: process.env.REACT_APP_API_KEY || 'your-super-secret-key-12345',
  orchestrator: process.env.REACT_APP_ORCH_URL || 'http://localhost:8006',
  contacts: process.env.REACT_APP_CONTACTS_URL || 'http://localhost:8002',
  campaigns: process.env.REACT_APP_CAMPAIGNS_URL || 'http://localhost:8005',
  connector: process.env.REACT_APP_CONNECTOR_URL || 'http://localhost:8008',
};

const okToast = (msg) => toast.success(msg, { autoClose: 500 });
const errToast = (msg) => toast.error(msg, { autoClose: 900 });
const infoToast = (msg) => toast.info(msg, { autoClose: 500 });

const ALLOWED_EXTS = ['csv', 'xlsx', 'txt'];
const MAX_FILE_MB = 25;
const MAX_TOTAL_MB = 100;
const REQUIRED_CSV_HEADERS = ['SENDER_NAME','SENDER_EMAIL']; // warn-only
const STORAGE_KEY = 'fileUpload.prefs';

const reqId = () => `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;

function FileUploadView() {
  const [files, setFiles] = useState([]);
  const [message, setMessage] = useState('');
  const [startDate, setStartDate] = useState('');
  const [endDate, setEndDate] = useState('');
  const [chunkDays, setChunkDays] = useState(7);
  const [autoClear, setAutoClear] = useState(false);
  const [loading, setLoading] = useState(false);
  const [isError, setIsError] = useState(false);
  const [progress, setProgress] = useState(0);
  const [dragOver, setDragOver] = useState(false);
  const [dryRun, setDryRun] = useState(false);
  const [online, setOnline] = useState(typeof navigator !== 'undefined' ? navigator.onLine : true);
  const [, startTransition] = useTransition();
  const inputRef = useRef(null);
  const controllerRef = useRef(null);

  // Service health
  const [svc, setSvc] = useState({
    orchestrator: { ok: null, ms: null },
    contacts: { ok: null, ms: null },
    campaigns: { ok: null, ms: null },
    connector: { ok: null, ms: null },
  });

  // CSV preview
  const [previewOpen, setPreviewOpen] = useState(false);
  const [previewRows, setPreviewRows] = useState({ headers: [], rows: [] });

  // Chunk preview
  const [chunkPreviewOpen, setChunkPreviewOpen] = useState(false);
  const [chunkPreviewRows, setChunkPreviewRows] = useState([]);

  // Processing monitors
  const [uploadHistory, setUploadHistory] = useState([]);
  const [batchHistory, setBatchHistory] = useState([]);

  // Load prefs
  useEffect(() => {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (!raw) return;
    try {
      const saved = JSON.parse(raw);
      if (saved && typeof saved === 'object') {
        if (saved.startDate) setStartDate(saved.startDate);
        if (saved.endDate) setEndDate(saved.endDate);
        if (typeof saved.dryRun === 'boolean') setDryRun(saved.dryRun);
        if (typeof saved.chunkDays === 'number') setChunkDays(saved.chunkDays);
        if (typeof saved.autoClear === 'boolean') setAutoClear(saved.autoClear);
      }
    } catch (e) {
      console.debug('prefs load skipped', e);
    }
  }, []);

  // Save prefs
  useEffect(() => {
    const payload = { startDate, endDate, dryRun, chunkDays, autoClear };
    localStorage.setItem(STORAGE_KEY, JSON.stringify(payload));
  }, [startDate, endDate, dryRun, chunkDays, autoClear]);

  // Online/offline banner
  useEffect(() => {
    const on = () => setOnline(true);
    const off = () => setOnline(false);
    window.addEventListener('online', on);
    window.addEventListener('offline', off);
    return () => {
      window.removeEventListener('online', on);
      window.removeEventListener('offline', off);
    };
  }, []);

  // Shortcuts
  const handlePullApiRef = useRef(null);
  const checkAllServicesRef = useRef(null);
  const cancelUploadRef = useRef(null);
  useEffect(() => {
    const onKey = (e) => {
      if (e.key === 'u') { e.preventDefault(); inputRef.current?.click(); }
      if (e.key === 's') { e.preventDefault(); handlePullApiRef.current && handlePullApiRef.current(); }
      if (e.key === 'h') { e.preventDefault(); checkAllServicesRef.current && checkAllServicesRef.current(); }
      if (e.key === 'Escape' && cancelUploadRef.current) { cancelUploadRef.current(); }
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, []);

  const minStart = useMemo(() => {
    const t = new Date();
    t.setDate(t.getDate() - 365);
    return t.toISOString().split('T')[0];
  }, []);

  const validateDates = useCallback(() => {
    if (!startDate || !endDate) return false;
    return new Date(startDate) <= new Date(endDate);
  }, [startDate, endDate]);

  const totalBytes = useMemo(() => files.reduce((acc, f) => acc + (f?.size || 0), 0), [files]);
  const totalMB = useMemo(() => (totalBytes / (1024 * 1024)), [totalBytes]);

  const mergeFiles = useCallback((list) => {
    const next = Array.from(list || []);
    const dedup = new Map(files.map(f => [f.name + f.size, f]));
    for (const f of next) {
      const ext = (f.name.split('.').pop() || '').toLowerCase();
      const mb = f.size / (1024 * 1024);
      if (!ALLOWED_EXTS.includes(ext)) { errToast(`Blocked: ${f.name} (.${ext} not allowed)`); continue; }
      if (mb > MAX_FILE_MB) { errToast(`Too large: ${f.name} (${mb.toFixed(1)} MB > ${MAX_FILE_MB} MB)`); continue; }
      const key = f.name + f.size;
      if (!dedup.has(key)) dedup.set(key, f);
    }
    const arr = Array.from(dedup.values());
    const newTotalMB = arr.reduce((acc, f) => acc + (f.size || 0), 0) / (1024 * 1024);
    if (newTotalMB > MAX_TOTAL_MB) {
      errToast(`Total size ${newTotalMB.toFixed(1)} MB exceeds limit ${MAX_TOTAL_MB} MB`);
      return;
    }
    setFiles(arr);
    setMessage(''); setIsError(false);
    infoToast(`${arr.length} file(s) selected`);

    // quick CSV header warn (first CSV only)
    const firstCsv = arr.find(f => (f.name.split('.').pop() || '').toLowerCase() === 'csv');
    if (firstCsv) {
      const reader = new FileReader();
      reader.onload = () => {
        const firstLine = String(reader.result || '').split(/\r?\n/)[0] || '';
        const missing = REQUIRED_CSV_HEADERS.filter(h => !firstLine.includes(h));
        if (missing.length) toast.warn(`CSV may be missing: ${missing.join(', ')}`);
      };
      reader.readAsText(firstCsv.slice(0, 1024 * 512));
    }
  }, [files]);

  const onFileChange = useCallback((e) => mergeFiles(e.target.files), [mergeFiles]);
  const onDrop = useCallback((e) => { e.preventDefault(); setDragOver(false); mergeFiles(e.dataTransfer.files); }, [mergeFiles]);
  const onDragOver = useCallback((e) => { e.preventDefault(); setDragOver(true); }, []);
  const onDragLeave = useCallback(() => setDragOver(false), []);
  const onPaste = useCallback((e) => {
    const text = e.clipboardData?.getData('text/plain') || '';
    if (!text || !/,|\t/.test(text)) return;
    const blob = new Blob([text], { type: 'text/csv' });
    const file = new File([blob], `pasted_${Date.now()}.csv`, { type: 'text/csv' });
    mergeFiles([file]);
    okToast('Pasted CSV captured as file');
  }, [mergeFiles]);

  const setPreset = useCallback((days) => {
    const end = new Date();
    const start = new Date();
    start.setDate(end.getDate() - (days - 1));
    setStartDate(start.toISOString().slice(0, 10));
    setEndDate(end.toISOString().slice(0, 10));
  }, []);

  const cancelUpload = useCallback(() => {
    const ctrl = controllerRef.current;
    if (ctrl && typeof ctrl.abort === 'function') {
      ctrl.abort();
    }
    controllerRef.current = null;
    setLoading(false);
    setProgress(0);
    infoToast('Upload canceled');
  }, []);
  cancelUploadRef.current = cancelUpload;

  const downloadSample = useCallback(() => {
    const rows = [
      ['SENDER_NAME','SENDER_EMAIL','SENDER_MOBILE','SENDER_COMPANY','QUERY','PRODUCT_NAME','CITY','STATE','COUNTRY'],
      ['Asha Sharma','asha@example.com','9876543210','Acme Pvt Ltd','Need 500 boxes','Corrugated Box','Mumbai','MH','India'],
      ['Ravi Verma','ravi@example.com','9123456780','Verma Traders','Quote for tapes','BOPP Tape','Jaipur','RJ','India'],
    ];
    const csv = rows
      .map(r => r.map(v => {
        const s = String(v ?? '');
        return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
      }).join(','))
      .join('\n');

    const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'sample_leads.csv';
    a.click();
    URL.revokeObjectURL(url);
    okToast('Sample CSV downloaded');
  }, []);

  const parseCsvPreview = useCallback((text, maxRows = 10) => {
    // light CSV parser that handles quotes for preview
    const rows = [];
    let row = [];
    let cur = '';
    let inQuotes = false;

    const pushCell = () => { row.push(cur); cur = ''; };
    const pushRow = () => { rows.push(row); row = []; };

    for (let i = 0; i < text.length; i++) {
      const ch = text[i];
      if (inQuotes) {
        if (ch === '"' && text[i + 1] === '"') { cur += '"'; i++; }
        else if (ch === '"') { inQuotes = false; }
        else { cur += ch; }
      } else {
        if (ch === '"') inQuotes = true;
        else if (ch === ',') pushCell();
        else if (ch === '\n') { pushCell(); pushRow(); if (rows.length >= maxRows) break; }
        else if (ch === '\r') { /* skip CR */ }
        else { cur += ch; }
      }
    }
    if (cur.length || row.length) { pushCell(); pushRow(); }
    const headers = rows[0] || [];
    const body = rows.slice(1);
    return { headers, rows: body };
  }, []);

  const openPreview = useCallback(() => {
    const firstCsv = files.find(f => (f.name.split('.').pop() || '').toLowerCase() === 'csv');
    if (!firstCsv) {
      errToast('No CSV file to preview');
      return;
    }
    const reader = new FileReader();
    reader.onload = () => {
      const text = String(reader.result || '');
      const { headers, rows } = parseCsvPreview(text, 10);
      setPreviewRows({ headers, rows });
      setPreviewOpen(true);
    };
    reader.onerror = () => {
      errToast('Failed to read CSV');
    };
    reader.readAsText(firstCsv);
  }, [files, parseCsvPreview]);

  const computeChunks = useCallback((s, e, days) => {
    const chunks = [];
    let cur = new Date(s);
    const end = new Date(e);
    while (cur <= end) {
      let endChunk = new Date(cur);
      endChunk.setDate(endChunk.getDate() + (days - 1));
      if (endChunk > end) endChunk = end;
      chunks.push({
        start_date: cur.toISOString().slice(0, 10),
        end_date: endChunk.toISOString().slice(0, 10),
      });
      cur.setDate(cur.getDate() + days);
    }
    return chunks;
  }, []);

  const exportChunks = useCallback(() => {
    if (!validateDates()) {
      errToast('Select a valid date range first');
      return;
    }
    const days = Math.max(1, Math.min(30, Number(chunkDays) || 7));
    const chunks = computeChunks(startDate, endDate, days);
    const blob = new Blob([JSON.stringify({ days_per_chunk: days, chunks }, null, 2)], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url; a.download = `date_chunks_${Date.now()}.json`; a.click();
    URL.revokeObjectURL(url);
    okToast('Exported chunks JSON');
  }, [startDate, endDate, chunkDays, validateDates, computeChunks]);

  const previewChunks = useCallback(() => {
    if (!validateDates()) {
      errToast('Select a valid date range first');
      return;
    }
    const days = Math.max(1, Math.min(30, Number(chunkDays) || 7));
    const chunks = computeChunks(startDate, endDate, days);
    setChunkPreviewRows(chunks);
    setChunkPreviewOpen(true);
  }, [startDate, endDate, chunkDays, validateDates, computeChunks]);

  const onFileUpload = useCallback(async () => {
    if (!files.length) {
      setMessage('Please select one or more files.');
      setIsError(true);
      errToast('No files selected');
      return;
    }
    setLoading(true); setProgress(0); setMessage(''); setIsError(false);
    infoToast('Uploading files...');

    const formData = new FormData();
    for (let i = 0; i < files.length; i++) formData.append('files', files[i]);

    const controller = new AbortController();
    controllerRef.current = controller;

    const rid = reqId();
    const startedAt = new Date();

    try {
      const { data } = await axios.post(
        `${cfg.connector}/leads/upload${dryRun ? '?dry_run=true' : ''}`,
        formData,
        {
          headers: { 'Content-Type': 'multipart/form-data', 'X-API-Key': cfg.API_KEY, 'X-Request-Id': rid },
          signal: controller.signal,
          onUploadProgress: (evt) => {
            if (evt.total) setProgress(Math.round((evt.loaded * 100) / evt.total));
          },
        }
      );

      const totalFiles = data.total_files ?? data.total_files_processed ?? files.length;
      const totalLeads = data.total_leads ?? data.total_leads_queued ?? data.accepted ?? 0;

      setUploadHistory((prev) => [
        {
          id: rid,
          at: startedAt.toLocaleString(),
          files: totalFiles,
          sizeMB: totalMB.toFixed(1),
          leads: totalLeads,
          status: 'success',
        },
        ...prev.slice(0, 19),
      ]);

      startTransition(() => {
        setMessage(`Upload complete. Files: ${totalFiles}. Leads queued: ${totalLeads}.`);
      });
      if (autoClear) setFiles([]);
      okToast('Files uploaded');
    } catch (error) {
      const msg = error.response?.data?.detail || error.message;

      setUploadHistory((prev) => [
        {
          id: rid,
          at: startedAt.toLocaleString(),
          files: files.length,
          sizeMB: totalMB.toFixed(1),
          leads: 0,
          status: 'error',
          error: String(msg || 'Upload failed'),
        },
        ...prev.slice(0, 19),
      ]);

      if (axios.isCancel?.(error) || error.name === 'CanceledError') {
        startTransition(() => { setMessage('Upload canceled'); setIsError(true); });
      } else {
        startTransition(() => { setMessage(`Error uploading: ${msg}`); setIsError(true); });
        let payload = null;
        try {
          payload = JSON.stringify(error.response?.data ?? { error: msg }, null, 2);
        } catch (e) {
          payload = JSON.stringify({ error: msg }, null, 2);
        }
        const blob = new Blob([payload], { type: 'application/json' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url; a.download = `upload_error_${Date.now()}.json`; a.click();
        URL.revokeObjectURL(url);
        toast.warn('Saved error details');
      }
      errToast('Upload failed');
    } finally {
      controllerRef.current = null;
      setLoading(false);
      setTimeout(() => setProgress(0), 600);
    }
  }, [files, dryRun, autoClear, totalMB]);

  const handlePullApi = useCallback(async () => {
    if (!validateDates()) {
      setMessage('Please select valid start and end dates.');
      setIsError(true);
      errToast('Invalid dates');
      return;
    }
    const days = Math.max(1, Math.min(30, Number(chunkDays) || 7));
    setLoading(true); setMessage(''); setIsError(false);
    infoToast('Scheduling batch fetch...');

    const dateChunks = computeChunks(startDate, endDate, days);
    const rid = reqId();
    const startedAt = new Date();

    try {
      await axios.post(
        `${cfg.connector}/indiamart/pull/batch${dryRun ? '?dry_run=true' : ''}`,
        dateChunks,
        { headers: { 'X-API-Key': cfg.API_KEY, 'X-Request-Id': rid } }
      );

      setBatchHistory((prev) => [
        {
          id: rid,
          at: startedAt.toLocaleString(),
          chunks: dateChunks,
          count: dateChunks.length,
          days,
          status: 'scheduled',
        },
        ...prev.slice(0, 19),
      ]);

      startTransition(() => setMessage(`Scheduled ${dateChunks.length} batch(es) of ${days} day(s) each.`));
      okToast('Pull scheduled');
    } catch (error) {
      const msg = error.response?.data?.detail || error.message;

      setBatchHistory((prev) => [
        {
          id: rid,
          at: startedAt.toLocaleString(),
          chunks: dateChunks,
          count: dateChunks.length,
          days,
          status: 'error',
          error: String(msg || 'Schedule failed'),
        },
        ...prev.slice(0, 19),
      ]);

      startTransition(() => { setMessage(`Error scheduling: ${msg}`); setIsError(true); });
      errToast(msg);
    } finally {
      setLoading(false);
    }
  }, [startDate, endDate, chunkDays, dryRun, validateDates, computeChunks]);
  handlePullApiRef.current = handlePullApi;

  const checkOne = useCallback(async (name, base) => {
    const t0 = performance.now();
    try {
      await axios.get(`${base}/health`, { headers: { 'X-API-Key': cfg.API_KEY }, timeout: 3000 });
      const ms = Math.round(performance.now() - t0);
      return { name, ok: true, ms };
    } catch (e) {
      const ms = Math.round(performance.now() - t0);
      return { name, ok: false, ms };
    }
  }, []);

  const checkAllServices = useCallback(async () => {
    const results = await Promise.all([
      checkOne('orchestrator', cfg.orchestrator),
      checkOne('contacts', cfg.contacts),
      checkOne('campaigns', cfg.campaigns),
      checkOne('connector', cfg.connector),
    ]);
    const next = {
      orchestrator: { ok: results[0].ok, ms: results[0].ms },
      contacts: { ok: results[1].ok, ms: results[1].ms },
      campaigns: { ok: results[2].ok, ms: results[2].ms },
      connector: { ok: results[3].ok, ms: results[3].ms },
    };
    setSvc(next);
    toast(results.every(r => r.ok) ? 'All services healthy' : 'Some services are down');
  }, [checkOne]);
  checkAllServicesRef.current = checkAllServices;

  return (
    <PageContainer>
      {!online && <Alert severity="warning" sx={{ mb: 2 }}>You are offline. Actions will fail until connection is restored.</Alert>}

      {loading && <LinearProgress sx={{ mb: 2 }} />}
      {progress > 0 && <LinearProgress variant="determinate" value={progress} sx={{ mb: 2 }} />}

      {/* Preflight / services */}
      <UiCard sx={{ mb: 2 }}>
        <Box display="flex" alignItems="center" gap={1.5} flexWrap="wrap">
          <Typography variant="subtitle2" sx={{ mr: 1, opacity: 0.8 }}>Services:</Typography>
          {(['orchestrator','contacts','campaigns','connector']).map(k => (
            <Chip
              key={k}
              label={`${k}${svc[k].ms != null ? ` ${svc[k].ms}ms` : ''}`}
              color={svc[k].ok == null ? 'default' : (svc[k].ok ? 'success' : 'error')}
              variant="outlined"
              size="small"
            />
          ))}
          <Button variant="outlined" size="small" onClick={checkAllServices}>Check services (h)</Button>
          <FormControlLabel
            control={<Switch checked={dryRun} onChange={(e) => setDryRun(e.target.checked)} />}
            label="Dry run"
            sx={{ ml: 1 }}
          />
        </Box>
      </UiCard>

      <Grid container spacing={2}>
        {/* Upload */}
        <Grid item xs={12} md={6}>
          <motion.div whileHover={{ y: -2 }}>
            <UiCard>
              <Typography variant="h6" sx={{ fontWeight: 700, mb: 1.5 }}>
                Upload Leads File
              </Typography>
              <Typography variant="body2" sx={{ opacity: 0.85, mb: 2 }}>
                Upload .csv, .xlsx, or .txt files (max {MAX_FILE_MB} MB each; total ≤ {MAX_TOTAL_MB} MB).
              </Typography>

              {/* Drag & drop zone */}
              <Box
                onDrop={onDrop}
                onDragOver={onDragOver}
                onDragLeave={onDragLeave}
                onPaste={onPaste}
                sx={{
                  border: '1px dashed rgba(255,255,255,0.35)',
                  borderRadius: 2,
                  p: 2,
                  mb: 2,
                  textAlign: 'center',
                  cursor: 'pointer',
                  transition: 'background 120ms ease',
                  background: dragOver ? 'rgba(255,255,255,0.06)' : 'transparent',
                }}
                onClick={() => inputRef.current?.click()}
                aria-label="Drag, drop or paste CSV here"
              >
                <Typography variant="body2">Drag & drop files, paste CSV, or click to choose</Typography>
                <input
                  ref={inputRef}
                  type="file"
                  hidden
                  onChange={onFileChange}
                  accept=".csv,.xlsx,.txt"
                  multiple
                  aria-label="Select lead files"
                />
              </Box>

              <Box display="flex" gap={2} alignItems="center" flexWrap="wrap" mb={1}>
                <Button variant="contained" onClick={() => inputRef.current?.click()} disabled={loading}>
                  Choose Files (u)
                </Button>
                <Button
                  variant="outlined"
                  onClick={onFileUpload}
                  disabled={loading || !files.length}
                  aria-label="Upload selected files"
                >
                  {loading ? <CircularProgress size={18} /> : 'Upload Files'}
                </Button>
                <Button
                  variant="text"
                  onClick={() => setFiles([])}
                  disabled={loading || !files.length}
                >
                  Clear
                </Button>
                <Button variant="text" onClick={downloadSample}>Download sample CSV</Button>
                {files.some(f => (f.name.split('.').pop() || '').toLowerCase() === 'csv') && (
                  <Button variant="text" onClick={openPreview}>Preview CSV</Button>
                )}
                {controllerRef.current && (
                  <Button color="error" variant="text" onClick={cancelUpload}>
                    Cancel upload (Esc)
                  </Button>
                )}
              </Box>

              {/* File stats */}
              <Box display="flex" gap={1} alignItems="center" flexWrap="wrap">
                <Chip size="small" label={`Files: ${files.length}`} variant="outlined" />
                <Chip size="small" label={`Total: ${totalMB.toFixed(1)} MB`} variant="outlined" />
                <FormControlLabel
                  control={<Switch checked={autoClear} onChange={(e) => setAutoClear(e.target.checked)} />}
                  label="Auto-clear after upload"
                />
              </Box>

              {!!files.length && (
                <Box mt={1} display="flex" gap={1} flexWrap="wrap">
                  {files.map((f, i) => (
                    <Chip
                      key={f.name + i}
                      label={`${f.name} (${Math.round((f.size/1024))} KB)`}
                      onDelete={() => setFiles(prev => prev.filter((_, idx) => idx !== i))}
                      size="small"
                      variant="outlined"
                    />
                  ))}
                </Box>
              )}
            </UiCard>
          </motion.div>
        </Grid>

        {/* Batch Pull */}
        <Grid item xs={12} md={6}>
          <motion.div whileHover={{ y: -2 }}>
            <UiCard>
              <Typography variant="h6" sx={{ fontWeight: 700, mb: 1.5 }}>
                Fetch from IndiaMART (Batch Pull)
              </Typography>

              <Box display="flex" gap={2} flexWrap="wrap" alignItems="center" mb={1}>
                <Button size="small" variant="outlined" onClick={() => setPreset(7)}>Last 7d</Button>
                <Button size="small" variant="outlined" onClick={() => setPreset(30)}>Last 30d</Button>
                <Tooltip title="Days per batch window (1–30)">
                  <TextField
                    size="small"
                    label="Days per chunk"
                    type="number"
                    value={chunkDays}
                    onChange={(e) => {
                      const n = Math.max(1, Math.min(30, Number(e.target.value) || 1));
                      setChunkDays(n);
                    }}
                    inputProps={{ min: 1, max: 30 }}
                    sx={{ width: 150 }}
                  />
                </Tooltip>
                <Button size="small" variant="text" onClick={previewChunks}>
                  Preview chunks
                </Button>
                <Button size="small" variant="text" onClick={exportChunks}>
                  Export chunks JSON
                </Button>
              </Box>

              <Grid container spacing={2}>
                <Grid item xs={12} sm={6}>
                  <TextField
                    label="Start Date"
                    type="date"
                    value={startDate}
                    onChange={(e) => setStartDate(e.target.value)}
                    inputProps={{ min: minStart, max: new Date().toISOString().slice(0, 10) }}
                    InputLabelProps={{ shrink: true }}
                    fullWidth
                    error={startDate && !validateDates()}
                    helperText={startDate && !validateDates() ? 'Invalid date range' : ''}
                    aria-label="Select start date"
                  />
                </Grid>
                <Grid item xs={12} sm={6}>
                  <TextField
                    label="End Date"
                    type="date"
                    value={endDate}
                    onChange={(e) => setEndDate(e.target.value)}
                    inputProps={{ min: startDate || minStart, max: new Date().toISOString().slice(0, 10) }}
                    InputLabelProps={{ shrink: true }}
                    fullWidth
                    error={endDate && !validateDates()}
                    helperText={endDate && !validateDates() ? 'Invalid date range' : ''}
                    aria-label="Select end date"
                  />
                </Grid>
              </Grid>

              <FormControlLabel
                sx={{ mt: 1 }}
                control={<Switch checked={dryRun} onChange={(e) => setDryRun(e.target.checked)} />}
                label="Dry run"
              />

              <Button
                sx={{ mt: 2 }}
                variant="contained"
                color="warning"
                onClick={handlePullApi}
                disabled={loading || !validateDates()}
                aria-label="Schedule batch fetch"
              >
                {loading ? <CircularProgress size={18} /> : 'Schedule Batch Fetch (s)'}
              </Button>
            </UiCard>
          </motion.div>
        </Grid>
      </Grid>

      {message && <Alert severity={isError ? 'error' : 'success'} sx={{ mt: 2 }}>{message}</Alert>}

      {/* Processing Monitor */}
      <UiCard sx={{ mt: 2 }}>
        <Typography variant="subtitle1" sx={{ fontWeight: 700, mb: 1 }}>
          Processing Monitor
        </Typography>

        <Typography variant="body2" sx={{ opacity: 0.85, mb: 0.5 }}>Recent uploads</Typography>
        <Table size="small" aria-label="Upload history">
          <TableHead>
            <TableRow>
              <TableCell>Time</TableCell>
              <TableCell>Request ID</TableCell>
              <TableCell align="right">Files</TableCell>
              <TableCell align="right">Size (MB)</TableCell>
              <TableCell align="right">Leads</TableCell>
              <TableCell>Status</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {uploadHistory.length === 0 ? (
              <TableRow><TableCell colSpan={6} sx={{ opacity: 0.7 }}>No uploads yet.</TableCell></TableRow>
            ) : uploadHistory.map((u) => (
              <TableRow key={u.id}>
                <TableCell>{u.at}</TableCell>
                <TableCell>{u.id}</TableCell>
                <TableCell align="right">{u.files}</TableCell>
                <TableCell align="right">{u.sizeMB}</TableCell>
                <TableCell align="right">{u.leads}</TableCell>
                <TableCell>
                  <Chip
                    size="small"
                    label={u.status === 'success' ? 'success' : 'error'}
                    color={u.status === 'success' ? 'success' : 'error'}
                    variant="outlined"
                  />
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>

        <Divider sx={{ my: 2 }} />

        <Typography variant="body2" sx={{ opacity: 0.85, mb: 0.5 }}>Recent batch pulls</Typography>
        <Table size="small" aria-label="Batch history">
          <TableHead>
            <TableRow>
              <TableCell>Time</TableCell>
              <TableCell>Request ID</TableCell>
              <TableCell align="right">Chunks</TableCell>
              <TableCell align="right">Days/chunk</TableCell>
              <TableCell>Status</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {batchHistory.length === 0 ? (
              <TableRow><TableCell colSpan={5} sx={{ opacity: 0.7 }}>No batch pulls yet.</TableCell></TableRow>
            ) : batchHistory.map((b) => (
              <TableRow key={b.id}>
                <TableCell>{b.at}</TableCell>
                <TableCell>{b.id}</TableCell>
                <TableCell align="right">{b.count}</TableCell>
                <TableCell align="right">{b.days}</TableCell>
                <TableCell>
                  <Chip
                    size="small"
                    label={b.status}
                    color={b.status === 'scheduled' ? 'info' : (b.status === 'error' ? 'error' : 'default')}
                    variant="outlined"
                  />
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </UiCard>

      {/* CSV Preview Dialog */}
      <Dialog open={previewOpen} onClose={() => setPreviewOpen(false)} maxWidth="md" fullWidth>
        <DialogTitle>CSV Preview (first 10 rows)</DialogTitle>
        <DialogContent dividers>
          {previewRows.headers.length ? (
            <Table size="small">
              <TableHead>
                <TableRow>
                  {previewRows.headers.map((h, i) => (
                    <TableCell key={i} sx={{ fontWeight: 700 }}>{String(h || '')}</TableCell>
                  ))}
                </TableRow>
              </TableHead>
              <TableBody>
                {previewRows.rows.map((r, ri) => (
                  <TableRow key={ri}>
                    {previewRows.headers.map((_, ci) => (
                      <TableCell key={ci}>{String(r[ci] ?? '')}</TableCell>
                    ))}
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          ) : (
            <Typography variant="body2">No rows to display.</Typography>
          )}
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setPreviewOpen(false)}>Close</Button>
        </DialogActions>
      </Dialog>

      {/* Chunk Preview Dialog */}
      <Dialog open={chunkPreviewOpen} onClose={() => setChunkPreviewOpen(false)} maxWidth="sm" fullWidth>
        <DialogTitle>Planned fetch windows</DialogTitle>
        <DialogContent dividers>
          {chunkPreviewRows.length ? (
            <Table size="small">
              <TableHead>
                <TableRow>
                  <TableCell>#</TableCell>
                  <TableCell>Start</TableCell>
                  <TableCell>End</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {chunkPreviewRows.map((c, i) => (
                  <TableRow key={`${c.start_date}_${c.end_date}_${i}`}>
                    <TableCell>{i + 1}</TableCell>
                    <TableCell>{c.start_date}</TableCell>
                    <TableCell>{c.end_date}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          ) : (
            <Typography variant="body2">No chunks to display.</Typography>
          )}
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setChunkPreviewOpen(false)}>Close</Button>
        </DialogActions>
      </Dialog>
    </PageContainer>
  );
}

export default React.memo(FileUploadView);
