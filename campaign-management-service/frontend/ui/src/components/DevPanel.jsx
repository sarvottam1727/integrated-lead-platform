import React, { useState, useEffect, useRef, useCallback, useMemo, useTransition } from 'react';
import axios from 'axios';
import { FaHeartbeat } from 'react-icons/fa';
import { Button, Chip, Typography, Grid, Box } from '@mui/material';
import { motion } from 'framer-motion';
import { toast } from 'react-toastify';
import { Terminal } from '@xterm/xterm';
import { FitAddon } from '@xterm/addon-fit';
import debounce from 'lodash.debounce';
import { Card, PageContainer } from './Common';

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

const DevPanel = React.memo(function DevPanel() {
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

export default DevPanel;
