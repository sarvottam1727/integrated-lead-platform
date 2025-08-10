import React, {
  useState,
  useEffect,
  useTransition,
  useDeferredValue,
  useCallback,
  useMemo,
  useRef,
} from 'react';
import axios from 'axios';
import {
  Table, TableHead, TableRow, TableCell, TableBody, Alert, LinearProgress,
  Box, Button, TextField, Select, MenuItem, FormControlLabel, Switch,
  Chip, Stack, TableContainer, TablePagination, Paper, Divider, Typography, Skeleton
} from '@mui/material';
import { toast } from 'react-toastify';

// ── inline minimal stand-ins to avoid importing from ./Common (prevents cycle) ──
const UiCard = ({ children, sx, ...rest }) => (
  <Box sx={{ p: 2, borderRadius: 2, background: 'transparent', ...sx }} {...rest}>{children}</Box>
);
const PageContainer = ({ children }) => <Box sx={{ p: 2 }}>{children}</Box>;
const SkeletonRow = ({ children }) => <TableRow>{children}</TableRow>;
// ──────────────────────────────────────────────────────────────────────────────

const cfg = {
  API_KEY: process.env.REACT_APP_API_KEY || 'your-super-secret-key-12345',
  orchestrator: process.env.REACT_APP_ORCH_URL || 'http://localhost:8006',
  contacts: process.env.REACT_APP_CONTACTS_URL || 'http://localhost:8002',
  campaigns: process.env.REACT_APP_CAMPAIGNS_URL || 'http://localhost:8005',
  connector: process.env.REACT_APP_CONNECTOR_URL || 'http://localhost:8011',
};

const okToast = (msg) => toast.success(msg, { autoClose: 500 });
const errToast = (msg) => toast.error(msg, { autoClose: 900 });

const STATUS_COLORS = {
  draft: 'default',
  scheduled: 'info',
  sending: 'warning',
  sent: 'success',
  failed: 'error',
};

const headers = ['ID', 'Campaign Name', 'Status', 'Sent', 'Failed', 'Opens', 'Clicks', 'Date'];

function CampaignHistory() {
  const [rows, setRows] = useState([]);
  const [loading, setLoading] = useState(false);
  const [msg, setMsg] = useState('');
  const [isError, setIsError] = useState(false);
  const [, startTransition] = useTransition();

  // UI state
  const [q, setQ] = useState('');
  const [status, setStatus] = useState('all');
  const [sortBy, setSortBy] = useState('created_at');
  const [sortDir, setSortDir] = useState('desc');
  const [page, setPage] = useState(0);
  const [rowsPerPage, setRowsPerPage] = useState(10);
  const [autoRefresh, setAutoRefresh] = useState(false);
  const [lastUpdated, setLastUpdated] = useState(null);

  const searchRef = useRef(null);

  const fetchCampaigns = useCallback(async () => {
    setLoading(true);
    setMsg('');
    setIsError(false);
    try {
      const { data } = await axios.get(`${cfg.campaigns}/campaigns`);
      startTransition(() => {
        setRows(Array.isArray(data) ? data : []);
        setLastUpdated(new Date());
      });
      okToast('Campaigns loaded');
    } catch (error) {
      startTransition(() => {
        setMsg(`Fetch failed: ${error.message}`);
        setIsError(true);
      });
      errToast(error.message);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchCampaigns();
  }, [fetchCampaigns]);

  useEffect(() => {
    if (!autoRefresh) return;
    const id = setInterval(fetchCampaigns, 15000);
    return () => clearInterval(id);
  }, [autoRefresh, fetchCampaigns]);

  useEffect(() => {
    const onKey = (e) => {
      if (e.key === 'r' && !e.metaKey && !e.ctrlKey && !e.altKey) {
        e.preventDefault();
        fetchCampaigns();
      }
      if (e.key === '/') {
        e.preventDefault();
        searchRef.current?.focus();
      }
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [fetchCampaigns]);

  const filteredSorted = useMemo(() => {
    const term = q.trim().toLowerCase();
    const statusFilter = (status || 'all').toLowerCase();

    const filtered = rows.filter((r) => {
      const matchesStatus = statusFilter === 'all'
        ? true
        : String(r.status || '').toLowerCase() === statusFilter;
      if (!term) return matchesStatus;
      const inId = String(r.id ?? '').toLowerCase().includes(term);
      const inName = String(r.name ?? '').toLowerCase().includes(term);
      return matchesStatus && (inId || inName);
    });

    const sorted = filtered.slice().sort((a, b) => {
      const dir = sortDir === 'asc' ? 1 : -1;
      const av = a?.[sortBy];
      const bv = b?.[sortBy];

      if (sortBy === 'created_at') {
        const ad = av ? new Date(av).getTime() : 0;
        const bd = bv ? new Date(bv).getTime() : 0;
        return (ad - bd) * dir;
      }
      if (['successful_sends', 'failed_sends', 'opens', 'clicks', 'id'].includes(sortBy)) {
        const an = Number(av ?? 0);
        const bn = Number(bv ?? 0);
        return (an - bn) * dir;
      }
      const as = String(av ?? '').toLowerCase();
      const bs = String(bv ?? '').toLowerCase();
      if (as < bs) return -1 * dir;
      if (as > bs) return 1 * dir;
      return 0;
    });

    return sorted;
  }, [rows, q, status, sortBy, sortDir]);

  const deferred = useDeferredValue(filteredSorted);

  const paged = useMemo(() => {
    const arr = Array.isArray(deferred) ? deferred : [];
    const start = page * rowsPerPage;
    return arr.slice(start, start + rowsPerPage);
  }, [deferred, page, rowsPerPage]);

  const stats = useMemo(() => {
    const arr = Array.isArray(deferred) ? deferred : [];
    const base = { count: arr.length, sent: 0, failed: 0, opens: 0, clicks: 0 };
    for (const r of arr) {
      base.sent += Number(r.successful_sends ?? 0);
      base.failed += Number(r.failed_sends ?? 0);
      base.opens += Number(r.opens ?? 0);
      base.clicks += Number(r.clicks ?? 0);
    }
    const openRate = base.sent > 0 ? (base.opens / base.sent) * 100 : 0;
    const clickRate = base.sent > 0 ? (base.clicks / base.sent) * 100 : 0;
    return { ...base, openRate, clickRate };
  }, [deferred]);

  const exportCsv = useCallback(() => {
    const arr = Array.isArray(deferred) ? deferred : [];
    const cols = ['id','name','status','successful_sends','failed_sends','opens','clicks','created_at'];
    const esc = (v) => {
      const s = v == null ? '' : String(v);
      return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
    };
    const lines = [
      cols.join(','),
      ...arr.map(r => cols.map(k => esc(k === 'created_at'
        ? (r[k] ? new Date(r[k]).toISOString() : '')
        : r[k])).join(',')),
    ];
    const blob = new Blob([lines.join('\n')], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `campaigns_${Date.now()}.csv`;
    a.click();
    URL.revokeObjectURL(url);
    okToast('Exported CSV');
  }, [deferred]);

  return (
    <PageContainer>
      {loading && <LinearProgress sx={{ mb: 2 }} />}
      {msg && <Alert severity={isError ? 'error' : 'success'} sx={{ mt: 2 }}>{msg}</Alert>}

      {/* Toolbar */}
      <UiCard sx={{ mb: 2 }}>
        <Box display="flex" flexWrap="wrap" alignItems="center" gap={1.5}>
          <TextField
            inputRef={searchRef}
            size="small"
            label="Search (/, focus)"
            placeholder="Search by ID or Name"
            value={q}
            onChange={(e) => { setQ(e.target.value); setPage(0); }}
            sx={{ minWidth: 220 }}
          />
          <Select
            size="small"
            value={status}
            onChange={(e) => { setStatus(e.target.value); setPage(0); }}
            sx={{ minWidth: 160 }}
          >
            <MenuItem value="all">All statuses</MenuItem>
            <MenuItem value="draft">Draft</MenuItem>
            <MenuItem value="scheduled">Scheduled</MenuItem>
            <MenuItem value="sending">Sending</MenuItem>
            <MenuItem value="sent">Sent</MenuItem>
            <MenuItem value="failed">Failed</MenuItem>
          </Select>

          <Select
            size="small"
            value={sortBy}
            onChange={(e) => setSortBy(e.target.value)}
            sx={{ minWidth: 170 }}
          >
            <MenuItem value="created_at">Sort by: Date</MenuItem>
            <MenuItem value="name">Sort by: Name</MenuItem>
            <MenuItem value="status">Sort by: Status</MenuItem>
            <MenuItem value="successful_sends">Sort by: Sent</MenuItem>
            <MenuItem value="failed_sends">Sort by: Failed</MenuItem>
            <MenuItem value="opens">Sort by: Opens</MenuItem>
            <MenuItem value="clicks">Sort by: Clicks</MenuItem>
            <MenuItem value="id">Sort by: ID</MenuItem>
          </Select>

          <Select
            size="small"
            value={sortDir}
            onChange={(e) => setSortDir(e.target.value)}
            sx={{ minWidth: 120 }}
          >
            <MenuItem value="asc">Asc</MenuItem>
            <MenuItem value="desc">Desc</MenuItem>
          </Select>

          <Divider flexItem orientation="vertical" sx={{ mx: 0.5 }} />

          <Button variant="contained" size="small" onClick={fetchCampaigns}>Refresh (r)</Button>
          <Button variant="outlined" size="small" onClick={exportCsv}>Export CSV</Button>
          <FormControlLabel
            sx={{ ml: 1 }}
            control={<Switch checked={autoRefresh} onChange={(e) => setAutoRefresh(e.target.checked)} />}
            label="Auto-refresh"
          />

          <Box sx={{ ml: 'auto' }}>
            <Stack direction="row" spacing={1} alignItems="center">
              <Chip label={`Rows: ${stats.count}`} size="small" />
              <Chip label={`Sent: ${stats.sent}`} size="small" color="success" variant="outlined" />
              <Chip label={`Failed: ${stats.failed}`} size="small" color="error" variant="outlined" />
              <Chip label={`Opens: ${stats.opens} (${stats.openRate.toFixed(1)}%)`} size="small" variant="outlined" />
              <Chip label={`Clicks: ${stats.clicks} (${stats.clickRate.toFixed(1)}%)`} size="small" variant="outlined" />
              {lastUpdated && (
                <Typography variant="caption" sx={{ opacity: 0.8 }}>
                  Updated: {lastUpdated.toLocaleTimeString()}
                </Typography>
              )}
            </Stack>
          </Box>
        </Box>
      </UiCard>

      {/* Table */}
      <UiCard>
        <TableContainer component={Paper} sx={{ background: 'transparent', maxHeight: 520 }}>
          <Table stickyHeader aria-label="Campaign history table">
            <TableHead>
              <TableRow>
                {headers.map((h) => (
                  <TableCell key={h} sx={{ color: '#c3d7ff', fontWeight: 700 }}>
                    {h}
                  </TableCell>
                ))}
              </TableRow>
            </TableHead>
            <TableBody>
              {loading ? (
                Array.from({ length: 5 }).map((_, i) => (
                  <SkeletonRow key={i}>
                    {Array.from({ length: 8 }).map((__, j) => (
                      <TableCell key={j}><Skeleton variant="text" /></TableCell>
                    ))}
                  </SkeletonRow>
                ))
              ) : paged.length === 0 ? (
                <TableRow>
                  <TableCell colSpan={8} sx={{ color: '#bbb', textAlign: 'center', py: 5 }}>
                    No campaigns match your filters.
                  </TableCell>
                </TableRow>
              ) : (
                paged.map((row, i) => (
                  <TableRow
                    key={row.id ?? i}
                    sx={{ backgroundColor: i % 2 ? 'rgba(255,255,255,0.03)' : 'transparent' }}
                    hover
                  >
                    <TableCell sx={{ color: '#fff' }}>{row.id}</TableCell>
                    <TableCell sx={{ color: '#fff' }}>{row.name}</TableCell>
                    <TableCell sx={{ color: '#fff' }}>
                      <Chip
                        size="small"
                        label={row.status}
                        color={STATUS_COLORS[String(row.status || '').toLowerCase()] || 'default'}
                        variant="outlined"
                      />
                    </TableCell>
                    <TableCell sx={{ color: '#fff' }}>{row.successful_sends}</TableCell>
                    <TableCell sx={{ color: '#fff' }}>{row.failed_sends}</TableCell>
                    <TableCell sx={{ color: '#fff' }}>{row.opens}</TableCell>
                    <TableCell sx={{ color: '#fff' }}>{row.clicks}</TableCell>
                    <TableCell sx={{ color: '#fff' }}>
                      {row.created_at ? new Date(row.created_at).toLocaleString() : ''}
                    </TableCell>
                  </TableRow>
                ))
              )}
            </TableBody>
          </Table>
        </TableContainer>

        {/* Pagination */}
        <TablePagination
          component="div"
          count={Array.isArray(deferred) ? deferred.length : 0}
          page={page}
          onPageChange={(_, p) => setPage(p)}
          rowsPerPage={rowsPerPage}
          onRowsPerPageChange={(e) => { setRowsPerPage(parseInt(e.target.value, 10)); setPage(0); }}
          rowsPerPageOptions={[5, 10, 25, 50]}
        />
      </UiCard>
    </PageContainer>
  );
}

export default React.memo(CampaignHistory);
