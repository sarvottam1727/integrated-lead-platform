// src/App.js
import React, {
  useState,
  useEffect,
  useRef,
  useCallback,
  useMemo,
  useTransition,
  useDeferredValue,
  Suspense,
  lazy,
} from 'react';
import { BrowserRouter as Router, Routes, Route, NavLink, useLocation } from 'react-router-dom';
import axios from 'axios';
import {
  FaUpload,
  FaBullhorn,
  FaCog,
  FaHistory,
  FaRedo,
  FaHeartbeat,
  FaExclamationCircle,
} from 'react-icons/fa';
import {
  Button,
  CircularProgress,
  TextField,
  Checkbox,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Grid,
  Paper,
  Typography,
  Box,
  LinearProgress,
  MenuItem,
  Select,
  Chip,
  Tooltip,
  ThemeProvider,
  createTheme,
  
  Alert,
} from '@mui/material';
import { styled } from '@mui/material/styles';
import { keyframes } from '@emotion/react';
import { motion, AnimatePresence, useMotionValue, useTransform } from 'framer-motion';
import { ToastContainer, toast } from 'react-toastify';
import { Terminal } from '@xterm/xterm';
import { FitAddon } from '@xterm/addon-fit';
import 'react-toastify/dist/ReactToastify.css';
import '@xterm/xterm/css/xterm.css';
import debounce from 'lodash.debounce';

// MUI Theme
const theme = createTheme({
  palette: {
    mode: 'dark',
    primary: { main: '#3b82f6' },
    secondary: { main: '#60a5fa' },
    error: { main: '#ff8585' },
    success: { main: '#4ade80' },
  },
  typography: {
    fontFamily: 'Inter, ui-sans-serif, system-ui, -apple-system, Roboto, Arial',
  },
  components: {
    MuiButton: {
      styleOverrides: {
        root: {
          textTransform: 'none',
          borderRadius: '8px',
        },
      },
    },
  },
});

// Central Config
const cfg = {
  API_KEY: process.env.REACT_APP_API_KEY || 'your-super-secret-key-12345',
  orchestrator: process.env.REACT_APP_ORCH_URL || 'http://localhost:8006',
  contacts: process.env.REACT_APP_CONTACTS_URL || 'http://localhost:8002',
  campaigns: process.env.REACT_APP_CAMPAIGNS_URL || 'http://localhost:8005',
  connector: process.env.REACT_APP_CONNECTOR_URL || 'http://localhost:8008',
};

// Axios Setup
axios.defaults.headers.common['X-API-Key'] = cfg.API_KEY;
const cache = new Map();
axios.interceptors.request.use(
  (config) => {
    if (config.method === 'get' && cache.has(config.url)) {
      const cached = cache.get(config.url);
      if (Date.now() - cached.timestamp < 60_000) {
        return Promise.reject({ __fromCache: true, data: cached.data, config });
      }
    }
    return config;
  },
  (error) => Promise.reject(error),
);
axios.interceptors.response.use(
  (response) => {
    if (response.config.method === 'get') {
      cache.set(response.config.url, { data: response.data, timestamp: Date.now() });
    }
    return response;
  },
  (error) => {
    if (error.__fromCache) {
      return Promise.resolve({ data: error.data, config: error.config });
    }
    return Promise.reject(error);
  },
);

// Global Styles

const pulse = keyframes`
  0% { transform: scale(1) }
  50% { transform: scale(1.04) }
  100% { transform: scale(1) }
`;
const stars = keyframes`
  from { background-position: 0 0, 0 0, 0 0; }
  to { background-position: 10000px 0, 20000px 0, 30000px 0; }
`;

const AppContainer = styled('div')`
  min-height: 100vh;
  color: #ffffff;
  background:
    radial-gradient(2px 2px at 20% 30%, rgba(255,255,255,0.15) 50%, transparent 51%) repeat,
    radial-gradient(2px 2px at 80% 50%, rgba(255,255,255,0.1) 50%, transparent 51%) repeat,
    radial-gradient(1px 1px at 50% 80%, rgba(255,255,255,0.07) 50%, transparent 51%) repeat,
    linear-gradient(135deg, #0b1020 0%, #181a2e 40%, #15152a 100%);
  background-size: 250px 250px, 300px 300px, 350px 350px, cover;
  animation: ${stars} 140s linear infinite;
  perspective: 1200px;
`;

const NavWrap = styled('div')`
  position: sticky;
  top: 0;
  z-index: 1000;
  padding: 12px;
  display: flex;
  justify-content: center;
  background: rgba(10, 12, 25, 0.65);
  backdrop-filter: blur(12px);
  border-bottom: 1px solid rgba(255,255,255,0.08);
`;

const NavInner = styled('nav')`
  display: grid;
  grid-auto-flow: column;
  gap: 10px;
  @media (max-width: 768px) {
    grid-auto-flow: row;
    justify-items: center;
  }
`;

const TiltItem = ({ to, icon, children }) => {
  const x = useMotionValue(0);
  const y = useMotionValue(0);
  const rotateX = useTransform(y, [-50, 50], [8, -8]);
  const rotateY = useTransform(x, [-50, 50], [-8, 8]);

  const onMove = (e) => {
    const rect = e.currentTarget.getBoundingClientRect();
    x.set(e.clientX - (rect.left + rect.width / 2));
    y.set(e.clientY - (rect.top + rect.height / 2));
  };

  return (
    <motion.div
      whileHover={{ scale: 1.03 }}
      style={{ rotateX, rotateY, transformStyle: 'preserve-3d' }}
      onMouseMove={onMove}
      onMouseLeave={() => {
        x.set(0);
        y.set(0);
      }}
    >
      <NavItem to={to} end={to === '/'}>
        <IconWrap>{icon}</IconWrap>
        <span style={{ transform: 'translateZ(30px)' }}>{children}</span>
      </NavItem>
    </motion.div>
  );
};

const NavItem = styled(NavLink)`
  color: #c3d7ff;
  text-decoration: none;
  display: inline-flex;
  align-items: center;
  padding: 12px 20px;
  border-radius: 12px;
  border: 1px solid rgba(255,255,255,0.08);
  background: linear-gradient(180deg, rgba(255,255,255,0.06), rgba(255,255,255,0.02));
  box-shadow: 0 10px 35px rgba(0,0,0,0.35), inset 0 1px 0 rgba(255,255,255,0.06);
  font-weight: 700;
  font-size: 18px;
  transition: all 0.15s ease;
  transform-style: preserve-3d;

  &.active {
    background: linear-gradient(45deg, #3b82f6, #60a5fa);
    color: #fff;
    border-color: transparent;
  }
  &:hover {
    color: #fff;
    border-color: rgba(59,130,246,0.6);
    box-shadow: 0 16px 40px rgba(59,130,246,0.45);
    animation: ${pulse} 0.25s ease;
  }
  @media (max-width: 768px) {
    width: 100%;
    justify-content: center;
  }
`;

const IconWrap = styled('span')`
  margin-right: 10px;
  display: inline-flex;
  transform: translateZ(40px);
`;

const Card = styled(Paper)`
  padding: 24px;
  background: radial-gradient(1200px 800px at -10% -30%, rgba(96,165,250,0.08), transparent 40%),
    radial-gradient(800px 400px at 110% 130%, rgba(59,130,246,0.08), transparent 40%),
    rgba(255,255,255,0.05);
  border-radius: 16px !important;
  border: 1px solid rgba(255,255,255,0.08);
  box-shadow: 0 20px 60px rgba(0,0,0,0.35), inset 0 1px 0 rgba(255,255,255,0.05);
  transform: translateZ(0);
`;

const StyledPage = styled(motion.div)`
  padding: 32px;
  margin: 24px;
  background: rgba(18,18,30,0.6);
  border: 1px solid rgba(255,255,255,0.06);
  border-radius: 16px;
  box-shadow: 0 12px 40px rgba(0,0,0,0.35);
  backdrop-filter: blur(10px);
  @media (max-width: 600px) {
    margin: 12px;
    padding: 16px;
  }
`;

const PageContainer = (props) => (
  <StyledPage
    initial={{ opacity: 0, y: 6 }}
    animate={{ opacity: 1, y: 0 }}
    exit={{ opacity: 0, y: -6 }}
    transition={{ duration: 0.12 }}
    {...props}
  />
);



const SkeletonRow = styled(TableRow)`
  background-color: rgba(255,255,255,0.03);
  & > td {
    padding: 16px;
    color: transparent;
    background: linear-gradient(
      90deg,
      rgba(255,255,255,0.04) 25%,
      rgba(255,255,255,0.09) 50%,
      rgba(255,255,255,0.04) 75%
    );
    background-size: 200% 100%;
    animation: ${keyframes`
      0% { background-position: 200% 0; }
      100% { background-position: -200% 0; }
    `} 0.6s infinite;
  }
`;

// Error Boundary
class ErrorBoundary extends React.Component {
  state = { error: null };
  static getDerivedStateFromError(error) {
    return { error };
  }
  render() {
    if (this.state.error) {
      return (
        <Card elevation={0} sx={{ m: 4, p: 4, textAlign: 'center' }}>
          <Typography variant="h6" color="error" gutterBottom>
            <FaExclamationCircle style={{ verticalAlign: 'middle', marginRight: 8 }} />
            Something went wrong
          </Typography>
          <Typography variant="body2" sx={{ mb: 2 }}>
            {this.state.error.message}
          </Typography>
          <Button variant="contained" onClick={() => window.location.reload()}>
            Reload Page
          </Button>
        </Card>
      );
    }
    return this.props.children;
  }
}

// Shared Utilities
const okToast = (msg) => toast.success(msg, { autoClose: 500 });
const errToast = (msg) => toast.error(msg, { autoClose: 900 });
const infoToast = (msg) => toast.info(msg, { autoClose: 500 });

// Dev Panel Component
const DevPanel = React.memo(() => {
  const [services, setServices] = useState({});
  const [health, setHealth] = useState({});
  const [logSource, setLogSource] = useState(null);
  const [, startTransition] = useTransition();
  const terminalRef = useRef(null);
  const term = useRef(null);
  const fitAddon = useRef(null);
  const ws = useRef(null);

  const fetchStatus = useCallback(async () => {
    try {
      const [statusRes, healthRes] = await Promise.all([
        axios.get(`${cfg.orchestrator}/services/status`),
        axios.get(`${cfg.orchestrator}/health`),
      ]);
      startTransition(() => {
        setServices(statusRes.data || {});
        setHealth(healthRes.data || {});
      });
    } catch (error) {
      errToast(`Status fetch failed: ${error.message}`);
    }
  }, []);

  useEffect(() => {
    term.current = new Terminal({
      convertEol: true,
      rows: 18,
      theme: { background: '#0b1020', foreground: '#ffffff', cursor: '#3b82f6' },
      fontSize: 14,
    });
    fitAddon.current = new FitAddon();
    term.current.loadAddon(fitAddon.current);
    term.current.open(terminalRef.current);
    fitAddon.current.fit();

    fetchStatus();
    const t = setInterval(fetchStatus, 1200);
    const resizeObserver = new ResizeObserver(() => fitAddon.current?.fit());
    resizeObserver.observe(terminalRef.current);
    return () => {
      clearInterval(t);
      resizeObserver.disconnect();
    };
  }, [fetchStatus]);

  useEffect(() => {
    if (ws.current) ws.current.close();
    if (!logSource) {
      term.current.clear();
      term.current.write('Select a service to view logs.');
      return;
    }
    term.current.clear();
    term.current.write(`Connecting to log stream for ${logSource}...\r\n`);

    ws.current = new WebSocket(`ws://${new URL(cfg.orchestrator).host}/ws/logs/${logSource}`);
    ws.current.onmessage = (event) => term.current.write(event.data.replace(/\n/g, '\r\n'));
    ws.current.onopen = () => infoToast('Log stream connected');
    ws.current.onclose = () => {
      term.current.write('\r\n--- Log stream disconnected ---\r\n');
      toast.warn('Log stream disconnected', { autoClose: 600 });
    };
    ws.current.onerror = () => ws.current.close();

    return () => {
      if (ws.current) ws.current.close();
    };
  }, [logSource]);

  const doAction = useCallback(
    (serviceName, action) => {
      const run = debounce(async () => {
        try {
          infoToast(`${action} → ${serviceName}`);
          await axios.post(`${cfg.orchestrator}/services/${serviceName}/${action}`);
          await fetchStatus();
          okToast(`${serviceName} ${action}ed`);
        } catch (error) {
          errToast(`Failed to ${action} ${serviceName}: ${error.message}`);
        }
      }, 60);
      run();
    },
    [fetchStatus],
  );

  const cards = useMemo(
    () => [
      { key: 'contacts', label: 'CONTACTS' },
      { key: 'indiamart', label: 'INDIAMART' },
      { key: 'email', label: 'EMAIL' },
      { key: 'campaigns', label: 'CAMPAIGNS' },
      { key: 'lead_worker', label: 'LEAD WORKER' },
      { key: 'frontend', label: 'FRONTEND' },
    ],
    [],
  );

  return (
    <PageContainer>
      <Typography variant="h4" gutterBottom sx={{ fontWeight: 800, fontSize: { xs: 24, md: 28 } }}>
        Developer Control Panel
      </Typography>
      <Grid container spacing={2}>
        {cards.map(({ key, label }) => {
          const procStatus = services[key]?.status || 'stopped';
          const healthStatus = health[key] || 'unknown';
          const running = procStatus === 'running';
          const healthy = running && healthStatus === 'healthy';
          const chipColor = healthy ? 'success' : running ? 'warning' : 'error';

          return (
            <Grid item xs={12} sm={6} md={4} key={key}>
              <motion.div whileHover={{ y: -4 }} transition={{ duration: 0.15 }}>
                <Card elevation={0} sx={{ minHeight: 160 }}>
                  <Typography variant="h6" sx={{ fontWeight: 700, mb: 1.2 }}>
                    {label}
                  </Typography>
                  <Box display="flex" alignItems="center" gap={1} sx={{ mb: 1 }}>
                    <Chip
                      label={running ? `Running (${healthStatus})` : 'Stopped'}
                      color={chipColor}
                      size="small"
                      icon={<FaHeartbeat />}
                    />
                  </Box>
                  <Grid container spacing={1}>
                    <Grid item>
                      <Button
                        size="small"
                        variant="contained"
                        onClick={() => doAction(key, 'start')}
                        disabled={running}
                        aria-label={`Start ${label} service`}
                      >
                        Start
                      </Button>
                    </Grid>
                    <Grid item>
                      <Button
                        size="small"
                        color="error"
                        variant="contained"
                        onClick={() => doAction(key, 'stop')}
                        disabled={!running}
                        aria-label={`Stop ${label} service`}
                      >
                        Stop
                      </Button>
                    </Grid>
                    <Grid item>
                      <Button
                        size="small"
                        variant="outlined"
                        onClick={() => setLogSource(key)}
                        disabled={!running}
                        aria-label={`View logs for ${label}`}
                      >
                        Logs
                      </Button>
                    </Grid>
                  </Grid>
                </Card>
              </motion.div>
            </Grid>
          );
        })}
      </Grid>
      <Typography variant="h6" sx={{ mt: 3, fontWeight: 700 }}>
        Live Logs: {logSource || 'None'}
      </Typography>
      <Card elevation={0} ref={terminalRef} sx={{ p: 1.5, mt: 1 }} />
    </PageContainer>
  );
});

// File Upload Component
const FileUpload = React.memo(() => {
  const [files, setFiles] = useState(null);
  const [message, setMessage] = useState('');
  const [startDate, setStartDate] = useState('');
  const [endDate, setEndDate] = useState('');
  const [loading, setLoading] = useState(false);
  const [isError, setIsError] = useState(false);
  const [, startTransition] = useTransition();

  const validateDates = useCallback(() => {
    if (!startDate || !endDate) return false;
    return new Date(startDate) <= new Date(endDate);
  }, [startDate, endDate]);

  const onFileChange = useCallback(
    (event) => {
      startTransition(() => {
        setFiles(event.target.files);
        setMessage('');
        setIsError(false);
      });
      infoToast(`${event.target.files.length} file(s) selected`);
    },
    [],
  );

  const onFileUpload = useCallback(async () => {
    if (!files || files.length === 0) {
      setMessage('Please select one or more files.');
      setIsError(true);
      return errToast('No files selected');
    }
    setLoading(true);
    setMessage('');
    setIsError(false);
    infoToast('Uploading files...');

    const formData = new FormData();
    for (let i = 0; i < files.length; i++) formData.append('files', files[i]);

    try {
      const { data } = await axios.post(`${cfg.connector}/leads/upload`, formData, {
        headers: { 'Content-Type': 'multipart/form-data' },
      });

      const totalFiles = data.total_files ?? data.total_files_processed ?? files.length;
      const totalLeads = data.total_leads ?? data.total_leads_queued ?? data.accepted ?? 0;
      startTransition(() => {
        setMessage(`Upload complete. Files: ${totalFiles}. Leads queued: ${totalLeads}.`);
      });
      okToast('Files uploaded');
    } catch (error) {
      const msg = error.response?.data?.detail || error.message;
      startTransition(() => {
        setMessage(`Error uploading: ${msg}`);
        setIsError(true);
      });
      errToast(msg);
    } finally {
      setLoading(false);
    }
  }, [files]);

  const handlePullApi = useCallback(async () => {
    if (!validateDates()) {
      setMessage('Please select valid start and end dates.');
      setIsError(true);
      return errToast('Invalid dates');
    }
    setLoading(true);
    setMessage('');
    setIsError(false);
    infoToast('Scheduling batch fetch...');

    const dateChunks = [];
    let cur = new Date(startDate);
    const end = new Date(endDate);
    while (cur <= end) {
      let endChunk = new Date(cur);
      endChunk.setDate(endChunk.getDate() + 6);
      if (endChunk > end) endChunk = end;
      dateChunks.push({
        start_date: cur.toISOString().slice(0, 10),
        end_date: endChunk.toISOString().slice(0, 10),
      });
      cur.setDate(cur.getDate() + 7);
    }

    try {
      await axios.post(`${cfg.connector}/indiamart/pull/batch`, dateChunks);
      startTransition(() => setMessage(`Scheduled ${dateChunks.length} batch(es).`));
      okToast('Pull scheduled');
    } catch (error) {
      const msg = error.response?.data?.detail || error.message;
      startTransition(() => {
        setMessage(`Error scheduling: ${msg}`);
        setIsError(true);
      });
      errToast(msg);
    } finally {
      setLoading(false);
    }
  }, [startDate, endDate, validateDates]);

  const minStart = useMemo(() => {
    const t = new Date();
    t.setDate(t.getDate() - 365);
    return t.toISOString().split('T')[0];
  }, []);

  return (
    <PageContainer>
      {loading && <LinearProgress sx={{ mb: 2 }} />}
      <Grid container spacing={2}>
        <Grid item xs={12} md={6}>
          <motion.div whileHover={{ y: -2 }}>
            <Card elevation={0}>
              <Typography variant="h6" sx={{ fontWeight: 700, mb: 1.5 }}>
                Upload Leads File
              </Typography>
              <Typography variant="body2" sx={{ opacity: 0.85, mb: 2 }}>
                Upload .csv, .xlsx, or .txt files. Phone and company are optional.
              </Typography>
              <Box display="flex" gap={2} alignItems="center" flexWrap="wrap">
                <Button variant="contained" component="label" disabled={loading}>
                  Choose Files
                  <input
                    type="file"
                    hidden
                    onChange={onFileChange}
                    accept=".csv,.xlsx,.txt"
                    multiple
                    aria-label="Select lead files"
                  />
                </Button>
                {files && <Typography variant="caption">{files.length} file(s) selected</Typography>}
                <Button
                  variant="outlined"
                  onClick={onFileUpload}
                  disabled={loading || !files}
                  aria-label="Upload selected files"
                >
                  {loading ? <CircularProgress size={18} /> : 'Upload Files'}
                </Button>
              </Box>
            </Card>
          </motion.div>
        </Grid>
        <Grid item xs={12} md={6}>
          <motion.div whileHover={{ y: -2 }}>
            <Card elevation={0}>
              <Typography variant="h6" sx={{ fontWeight: 700, mb: 1.5 }}>
                Fetch from IndiaMART (Batch Pull)
              </Typography>
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
              <Button
                sx={{ mt: 2 }}
                variant="contained"
                color="warning"
                onClick={handlePullApi}
                disabled={loading || !validateDates()}
                aria-label="Schedule batch fetch"
              >
                {loading ? <CircularProgress size={18} /> : 'Schedule Batch Fetch'}
              </Button>
            </Card>
          </motion.div>
        </Grid>
      </Grid>
      {message && <Alert severity={isError ? 'error' : 'success'} sx={{ mt: 2 }}>{message}</Alert>}
    </PageContainer>
  );
});

// Campaign Manager Component
const CampaignManager = React.memo(() => {
  const [campaignName, setCampaignName] = useState('');
  const [subject, setSubject] = useState('');
  const [body, setBody] = useState('');
  const [contacts, setContacts] = useState([]);
  const [selectedContacts, setSelectedContacts] = useState([]);
  const [message, setMessage] = useState('');
  const [isError, setIsError] = useState(false);
  const [loading, setLoading] = useState(true);
  const [scheduleDate, setScheduleDate] = useState('');
  const [page, setPage] = useState(1);
  const [contactsPerPage, setContactsPerPage] = useState(50);
  const [totalContacts, setTotalContacts] = useState(0);
  const [, startTransition] = useTransition();
  const deferredContacts = useDeferredValue(contacts);
  const ws = useRef(null);

  const validateForm = useCallback(() => {
    return campaignName.trim() && subject.trim() && body.trim() && selectedContacts.length > 0;
  }, [campaignName, subject, body, selectedContacts]);

  const debouncedSetCampaignName = useMemo(() => debounce((v) => setCampaignName(v), 60), []);
  const debouncedSetSubject = useMemo(() => debounce((v) => setSubject(v), 60), []);
  const debouncedSetBody = useMemo(() => debounce((v) => setBody(v), 60), []);
  const debouncedSetScheduleDate = useMemo(() => debounce((v) => setScheduleDate(v), 60), []);

  const fetchContacts = useCallback(async () => {
    setLoading(true);
    try {
      const url = `${cfg.contacts}/contacts?page=${page}&per_page=${contactsPerPage}`;
      const { data } = await axios.get(url);
      startTransition(() => {
        const list = data.contacts || [];
        setContacts(list);
        setTotalContacts(data.total || list.length || 0);
        setLoading(false);
      });
      okToast(`Loaded ${data.contacts?.length ?? 0} contacts`);
    } catch (err) {
      const errorMsg = err.response?.data?.detail || err.message;
      startTransition(() => {
        setMessage(`Failed to fetch contacts: ${errorMsg}`);
        setIsError(true);
        setLoading(false);
      });
      errToast(errorMsg);
    }
  }, [page, contactsPerPage]);

  useEffect(() => {
    let retry = 0;
    const maxRetries = 5;
    let reconnectTimeout;

    const connect = () => {
      ws.current = new WebSocket(`ws://${new URL(cfg.contacts).host}/ws/contacts`);
      ws.current.onopen = () => {
        infoToast('Contacts WS connected');
        fetchContacts();
      };
      ws.current.onmessage = (event) => {
        try {
          const { event: evt, data } = JSON.parse(event.data);
          startTransition(() => {
            if (evt === 'contacts_added') {
              infoToast(`Added ${data.created_count} contacts`);
              fetchContacts();
            } else if (evt === 'contact_updated') {
              setContacts((prev) => prev.map((c) => (c.id === data.id ? { ...c, ...data } : c)));
            } else if (evt === 'contact_deleted') {
              setContacts((prev) => prev.filter((c) => c.id !== data.id));
              setTotalContacts((t) => t - 1);
            }
          });
        } catch (e) {
          errToast(`WS parse error: ${e.message}`);
        }
      };
      ws.current.onclose = () => {
        if (retry < maxRetries) {
          retry++;
          reconnectTimeout = setTimeout(connect, 500 * retry);
        } else {
          toast.warn('WS failed, falling back to HTTP', { autoClose: 600 });
          fetchContacts();
        }
      };
      ws.current.onerror = () => ws.current.close();
    };

    connect();
    fetchContacts();
    return () => {
      if (ws.current) ws.current.close();
      clearTimeout(reconnectTimeout);
    };
  }, [fetchContacts]);

  const handleSelectAllClick = useCallback(
    (e) => {
      startTransition(() => {
        if (e.target.checked && deferredContacts) {
          const ids = deferredContacts.filter((c) => !c.dnc).map((n) => n.id);
          setSelectedContacts(ids);
          return;
        }
        setSelectedContacts([]);
      });
    },
    [deferredContacts],
  );

  const handleCheckboxClick = useCallback(
    (e, id) => {
      startTransition(() => {
        setSelectedContacts((prev) => (prev.includes(id) ? prev.filter((x) => x !== id) : [...prev, id]));
      });
    },
    [],
  );

  const handleDncToggle = useCallback(
    async (contactId) => {
      try {
        startTransition(() => {
          setContacts((prev) => prev.map((c) => (c.id === contactId ? { ...c, dnc: !c.dnc } : c)));
        });
        await axios.put(`${cfg.contacts}/contacts/${contactId}/dnc`);
        okToast('DNC updated');
      } catch (error) {
        const msg = error.response?.data?.detail || error.message;
        startTransition(() => {
          setMessage(`DNC update failed: ${msg}`);
          setIsError(true);
        });
        errToast(msg);
      }
    },
    [],
  );

  const handleSubmit = useCallback(async () => {
    if (!validateForm()) {
      setMessage('Fill all required fields and select at least one contact.');
      setIsError(true);
      return errToast('Missing fields');
    }
    setLoading(true);
    setMessage('');
    setIsError(false);
    infoToast('Sending campaign...');
    try {
      await axios.post(`${cfg.campaigns}/campaigns/send-now`, {
        name: campaignName,
        subject,
        body_template: body,
        contact_ids: selectedContacts,
      });
      okToast('Campaign sent');
      setCampaignName('');
      setSubject('');
      setBody('');
      setSelectedContacts([]);
    } catch (err) {
      const msg = err.response?.data?.detail || err.message;
      setMessage(`Error sending campaign: ${msg}`);
      setIsError(true);
      errToast(msg);
    } finally {
      setLoading(false);
    }
  }, [campaignName, subject, body, selectedContacts, validateForm]);

  const handleSchedule = useCallback(async () => {
    if (!validateForm() || !scheduleDate) {
      setMessage('To schedule, fill all fields and choose a date/time.');
      setIsError(true);
      return errToast('Missing schedule info');
    }
    setLoading(true);
    setMessage('');
    setIsError(false);
    infoToast('Scheduling...');
    try {
      await axios.post(`${cfg.campaigns}/campaigns/schedule`, {
        name: campaignName,
        subject,
        body_template: body,
        contact_ids: selectedContacts,
        scheduled_at: scheduleDate,
      });
      okToast(`Scheduled for ${new Date(scheduleDate).toLocaleString()}`);
      setCampaignName('');
      setSubject('');
      setBody('');
      setSelectedContacts([]);
      setScheduleDate('');
    } catch (error) {
      const msg = error.response?.data?.detail || error.message;
      setMessage(`Schedule error: ${msg}`);
      setIsError(true);
      errToast(msg);
    } finally {
      setLoading(false);
    }
  }, [campaignName, subject, body, selectedContacts, scheduleDate, validateForm]);

  const handleRetryFetch = useCallback(() => {
    setMessage('');
    setIsError(false);
    infoToast('Retrying fetch...');
    fetchContacts();
  }, [fetchContacts]);

  const isSelected = useCallback((id) => selectedContacts.includes(id), [selectedContacts]);

  const totalPages = useMemo(
    () => (deferredContacts && totalContacts ? Math.ceil(totalContacts / contactsPerPage) : 1),
    [deferredContacts, totalContacts, contactsPerPage],
  );

  const tableRows = useMemo(() => {
    if (loading || !deferredContacts || deferredContacts.length === 0) {
      return (
        <>
          {Array.from({ length: 5 }).map((_, i) => (
            <SkeletonRow key={i}>
              <TableCell padding="checkbox">
                <Checkbox disabled />
              </TableCell>
              <TableCell>Loading...</TableCell>
              <TableCell>Loading...</TableCell>
              <TableCell>
                <Button disabled>Loading...</Button>
              </TableCell>
            </SkeletonRow>
          ))}
          {!loading && deferredContacts && deferredContacts.length === 0 && (
            <TableRow>
              <TableCell colSpan={4} sx={{ color: '#ff8585', textAlign: 'center' }}>
                No contacts available. Please ingest or retry.
              </TableCell>
            </TableRow>
          )}
        </>
      );
    }
    return deferredContacts.map((contact, index) => {
      const selected = isSelected(contact.id);
      const rowStyle = contact.dnc
        ? { backgroundColor: 'rgba(255, 107, 107, 0.12)', opacity: 0.8 }
        : { backgroundColor: index % 2 === 0 ? 'rgba(255,255,255,0.02)' : 'rgba(255,255,255,0.04)' };
      return (
        <TableRow hover key={contact.id} sx={rowStyle}>
          <TableCell padding="checkbox">
            <Checkbox
              checked={selected}
              disabled={contact.dnc}
              onClick={(e) => handleCheckboxClick(e, contact.id)}
              inputProps={{ 'aria-label': `Select contact ${contact.name}` }}
            />
          </TableCell>
          <TableCell sx={{ color: '#fff' }}>{contact.name}</TableCell>
          <TableCell sx={{ color: '#fff' }}>{contact.email}</TableCell>
          <TableCell>
            <Button
              size="small"
              variant="outlined"
              color={contact.dnc ? 'success' : 'error'}
              onClick={() => handleDncToggle(contact.id)}
              aria-label={contact.dnc ? `Allow contact ${contact.name}` : `Mark ${contact.name} as DNC`}
            >
              {contact.dnc ? 'Allow' : 'DNC'}
            </Button>
          </TableCell>
        </TableRow>
      );
    });
  }, [deferredContacts, loading, isSelected, handleCheckboxClick, handleDncToggle]);

  return (
    <PageContainer>
      {loading && <LinearProgress sx={{ mb: 2 }} />}
      <Grid container spacing={2}>
        <Grid item xs={12} md={7}>
          <Card elevation={0}>
            <Box display="flex" justifyContent="space-between" alignItems="center" mb={2}>
              <Typography variant="h6" sx={{ fontWeight: 700 }}>
                Select Contacts ({selectedContacts.length} /{' '}
                {deferredContacts ? deferredContacts.filter((c) => !c.dnc).length : 0})
              </Typography>
              <Box display="flex" gap={2} alignItems="center">
                <Select
                  value={contactsPerPage}
                  onChange={(e) => setContactsPerPage(Number(e.target.value))}
                  sx={{ color: '#fff', bgcolor: 'rgba(255,255,255,0.05)' }}
                  aria-label="Contacts per page"
                >
                  <MenuItem value={10}>10 per page</MenuItem>
                  <MenuItem value={50}>50 per page</MenuItem>
                  <MenuItem value={100}>100 per page</MenuItem>
                </Select>
                <Tooltip title="Refetch contacts">
                  <Button
                    variant="outlined"
                    startIcon={<FaRedo />}
                    onClick={handleRetryFetch}
                    aria-label="Refetch contacts"
                  >
                    Retry
                  </Button>
                </Tooltip>
              </Box>
            </Box>
            <TableContainer sx={{ maxHeight: 440 }}>
              <Table stickyHeader aria-label="Contacts table">
                <TableHead>
                  <TableRow>
                    <TableCell padding="checkbox" sx={{ bgcolor: 'rgba(255,255,255,0.06)' }}>
                      <Checkbox
                        indeterminate={
                          deferredContacts &&
                          selectedContacts.length > 0 &&
                          selectedContacts.length < deferredContacts.filter((c) => !c.dnc).length
                        }
                        checked={
                          deferredContacts &&
                          deferredContacts.length > 0 &&
                          selectedContacts.length === deferredContacts.filter((c) => !c.dnc).length
                        }
                        onChange={handleSelectAllClick}
                        inputProps={{ 'aria-label': 'Select all contacts' }}
                      />
                    </TableCell>
                    <TableCell sx={{ bgcolor: 'rgba(255,255,255,0.06)', color: '#c3d7ff', fontWeight: 700 }}>
                      Name
                    </TableCell>
                    <TableCell sx={{ bgcolor: 'rgba(255,255,255,0.06)', color: '#c3d7ff', fontWeight: 700 }}>
                      Email
                    </TableCell>
                    <TableCell sx={{ bgcolor: 'rgba(255,255,255,0.06)', color: '#c3d7ff', fontWeight: 700 }}>
                      Actions
                    </TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>{tableRows}</TableBody>
              </Table>
            </TableContainer>
            <Box display="flex" justifyContent="space-between" mt={2}>
              <Button disabled={page === 1 || loading} onClick={() => setPage(page - 1)} aria-label="Previous page">
                Previous
              </Button>
              <Typography>
                Page {page} of {totalPages} (Total: {totalContacts})
              </Typography>
              <Button
                disabled={page === totalPages || loading}
                onClick={() => setPage(page + 1)}
                aria-label="Next page"
              >
                Next
              </Button>
            </Box>
          </Card>
        </Grid>
        <Grid item xs={12} md={5}>
          <Card elevation={0}>
            <Typography variant="h6" sx={{ fontWeight: 700 }}>
              Compose Campaign
            </Typography>
            <TextField
              label="Campaign Name"
              onChange={(e) => debouncedSetCampaignName(e.target.value)}
              fullWidth
              margin="normal"
              error={!campaignName.trim()}
              helperText={!campaignName.trim() ? 'Campaign name is required' : ''}
              inputProps={{ 'aria-label': 'Campaign name' }}
            />
            <TextField
              label="Email Subject"
              onChange={(e) => debouncedSetSubject(e.target.value)}
              fullWidth
              margin="normal"
              error={!subject.trim()}
              helperText={!subject.trim() ? 'Subject is required' : ''}
              inputProps={{ 'aria-label': 'Email subject' }}
            />
            <TextField
              label="Email Body (use {{ contact.name }})"
              multiline
              rows={4}
              onChange={(e) => debouncedSetBody(e.target.value)}
              fullWidth
              margin="normal"
              error={!body.trim()}
              helperText={!body.trim() ? 'Email body is required' : ''}
              inputProps={{ 'aria-label': 'Email body' }}
            />
            <TextField
              label="Schedule For (Optional)"
              type="datetime-local"
              onChange={(e) => debouncedSetScheduleDate(e.target.value)}
              InputLabelProps={{ shrink: true }}
              fullWidth
              margin="normal"
              inputProps={{ 'aria-label': 'Schedule date and time' }}
            />
            <Grid container spacing={2} sx={{ mt: 1 }}>
              <Grid item xs={6}>
                <Button
                  onClick={handleSubmit}
                  variant="contained"
                  disabled={loading || !validateForm()}
                  fullWidth
                  aria-label={`Send campaign to ${selectedContacts.length} contacts`}
                >
                  {loading ? <CircularProgress size={18} /> : `Send Now (${selectedContacts.length})`}
                </Button>
              </Grid>
              <Grid item xs={6}>
                <Button
                  onClick={handleSchedule}
                  variant="outlined"
                  disabled={loading || !validateForm() || !scheduleDate}
                  fullWidth
                  aria-label="Schedule campaign"
                >
                  {loading ? <CircularProgress size={18} /> : 'Schedule'}
                </Button>
              </Grid>
            </Grid>
          </Card>
        </Grid>
      </Grid>
      {message && <Alert severity={isError ? 'error' : 'success'} sx={{ mt: 2 }}>{message}</Alert>}
    </PageContainer>
  );
});

// Campaign History Component
const CampaignHistory = React.memo(() => {
  const [rows, setRows] = useState([]);
  const [loading, setLoading] = useState(false);
  const [msg, setMsg] = useState('');
  const [isError, setIsError] = useState(false);
  const [, startTransition] = useTransition();
  const deferred = useDeferredValue(rows);

  const fetchCampaigns = useCallback(async () => {
    setLoading(true);
    setMsg('');
    setIsError(false);
    try {
      const { data } = await axios.get(`${cfg.campaigns}/campaigns`);
      startTransition(() => setRows(data || []));
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

  return (
    <PageContainer>
      {loading && <LinearProgress sx={{ mb: 2 }} />}
      {msg && <Alert severity={isError ? 'error' : 'success'} sx={{ mt: 2 }}>{msg}</Alert>}
      <Card elevation={0}>
        <Table aria-label="Campaign history table">
          <TableHead>
            <TableRow>
              {['ID', 'Campaign Name', 'Status', 'Sent', 'Failed', 'Opens', 'Clicks', 'Date'].map((h) => (
                <TableCell key={h} sx={{ color: '#c3d7ff', fontWeight: 700 }}>
                  {h}
                </TableCell>
              ))}
            </TableRow>
          </TableHead>
          <TableBody>
            {loading || !deferred ? (
              Array.from({ length: 5 }).map((_, i) => (
                <SkeletonRow key={i}>
                  {Array.from({ length: 8 }).map((__, j) => (
                    <TableCell key={j}>Loading...</TableCell>
                  ))}
                </SkeletonRow>
              ))
            ) : (
              deferred.map((row, i) => (
                <TableRow key={row.id} sx={{ backgroundColor: i % 2 ? 'rgba(255,255,255,0.03)' : 'transparent' }}>
                  <TableCell sx={{ color: '#fff' }}>{row.id}</TableCell>
                  <TableCell sx={{ color: '#fff' }}>{row.name}</TableCell>
                  <TableCell sx={{ color: '#fff' }}>{row.status}</TableCell>
                  <TableCell sx={{ color: '#fff' }}>{row.successful_sends}</TableCell>
                  <TableCell sx={{ color: '#fff' }}>{row.failed_sends}</TableCell>
                  <TableCell sx={{ color: '#fff' }}>{row.opens}</TableCell>
                  <TableCell sx={{ color: '#fff' }}>{row.clicks}</TableCell>
                  <TableCell sx={{ color: '#fff' }}>{new Date(row.created_at).toLocaleString()}</TableCell>
                </TableRow>
              ))
            )}
          </TableBody>
        </Table>
      </Card>
    </PageContainer>
  );
});

// Lazy-loaded Components
const LazyDevPanel = lazy(() => Promise.resolve({ default: DevPanel }));
const LazyFileUpload = lazy(() => Promise.resolve({ default: FileUpload }));
const LazyCampaignManager = lazy(() => Promise.resolve({ default: CampaignManager }));
const LazyCampaignHistory = lazy(() => Promise.resolve({ default: CampaignHistory }));

// App Routes
function AppRoutes() {
  const location = useLocation();
  return (
    <AnimatePresence mode="wait">
      <Routes location={location} key={location.pathname}>
        <Route
          path="/upload"
          element={
            <Suspense fallback={<LinearProgress />}>
              <LazyFileUpload />
            </Suspense>
          }
        />
        <Route
          path="/history"
          element={
            <Suspense fallback={<LinearProgress />}>
              <LazyCampaignHistory />
            </Suspense>
          }
        />
        <Route
          path="/dev-panel"
          element={
            <Suspense fallback={<LinearProgress />}>
              <LazyDevPanel />
            </Suspense>
          }
        />
        <Route
          path="/"
          element={
            <Suspense fallback={<LinearProgress />}>
              <LazyCampaignManager />
            </Suspense>
          }
        />
      </Routes>
    </AnimatePresence>
  );
}

// Main App
function App() {
  return (
    <ThemeProvider theme={theme}>
      <AppContainer>
        <ToastContainer position="top-right" autoClose={350} theme="dark" hideProgressBar closeOnClick />
        <ErrorBoundary>
          <Router>
            <NavWrap>
              <NavInner>
                <TiltItem to="/" icon={<FaBullhorn />}>
                  Campaign Manager
                </TiltItem>
                <TiltItem to="/upload" icon={<FaUpload />}>
                  Upload & Fetch
                </TiltItem>
                <TiltItem to="/history" icon={<FaHistory />}>
                  Campaign History
                </TiltItem>
                <TiltItem to="/dev-panel" icon={<FaCog />}>
                  Dev Panel
                </TiltItem>
              </NavInner>
            </NavWrap>
            <AppRoutes />
          </Router>
        </ErrorBoundary>
      </AppContainer>
    </ThemeProvider>
  );
}

export default App;