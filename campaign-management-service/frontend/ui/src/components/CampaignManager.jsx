import React, { useState, useEffect, useRef, useCallback, useMemo, useTransition, useDeferredValue } from 'react';
import axios from 'axios';
import {
  Button, TextField, Checkbox, Table, TableBody, TableCell, TableContainer,
  TableHead, TableRow, Grid, Typography, Box, LinearProgress, MenuItem, Select,
  Alert, CircularProgress
} from '@mui/material';
import { toast } from 'react-toastify';
import debounce from 'lodash.debounce';
import { Card, PageContainer, SkeletonRow } from './Common';

const cfg = {
  API_KEY: process.env.REACT_APP_API_KEY || 'your-super-secret-key-12345',
  orchestrator: process.env.REACT_APP_ORCH_URL || 'http://localhost:8006',
  contacts: process.env.REACT_APP_CONTACTS_URL || 'http://localhost:8002',
  campaigns: process.env.REACT_APP_CAMPAIGNS_URL || 'http://localhost:8005',
  connector: process.env.REACT_APP_CONNECTOR_URL || 'http://localhost:8011', // ✅ 8011
};

const okToast = (msg) => toast.success(msg, { autoClose: 500 });
const errToast = (msg, opts = {}) => toast.error(msg, { autoClose: 900, ...opts });
const infoToast = (msg) => toast.info(msg, { autoClose: 500 });

// ws:// → wss:// helper
const toWsURL = (httpUrl, path) => {
  const u = new URL(httpUrl);
  u.protocol = u.protocol === 'https:' ? 'wss:' : 'ws:';
  u.pathname = path;
  u.search = '';
  return u.toString();
};

// personalization tokens accepted anywhere (subject or body)
const PERSONALIZATION_TOKENS = [
  '{{ contact.name }}',
  '{{ contact.email }}',
  '{{ contact.city }}',
  '{{ contact.state }}',
  '{{ contact.company }}',
];

const escapeReg = (s) => s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
const hasAnyToken = (text = '') => PERSONALIZATION_TOKENS.some(tok => new RegExp(escapeReg(tok)).test(text));
const listTokensUsed = (text = '') => PERSONALIZATION_TOKENS.filter(tok => new RegExp(escapeReg(tok)).test(text));

const CampaignManager = React.memo(function CampaignManager() {
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

  // Outcomes console + campaigns WS
  const outcomesWs = useRef(null);
  const [outcomes, setOutcomes] = useState([]);

  // A/B Subject & targeting
  const [subjectB, setSubjectB] = useState('');
  const [splitPercent, setSplitPercent] = useState(20);
  const [query, setQuery] = useState('');
  const [hideDnc, setHideDnc] = useState(true);

  // Preview drawer
  const [previewOpen, setPreviewOpen] = useState(false);
  const [previewContact, setPreviewContact] = useState(null);

  // Refs for live values (avoid debounce lag)
  const campaignNameRef = useRef(null);
  const subjectRef = useRef(null);
  const bodyRef = useRef(null);
  const scheduleRef = useRef(null);
  const searchRef = useRef(null);
  const activeFieldRef = useRef('body');

  const getCurrentValues = useCallback(() => {
    return {
      campaign: campaignNameRef.current?.value ?? campaignName,
      subj: subjectRef.current?.value ?? subject,
      subjB: subjectB,
      bodyText: bodyRef.current?.value ?? body,
      sched: scheduleRef.current?.value ?? scheduleDate,
    };
  }, [campaignName, subject, subjectB, body, scheduleDate]);

  // Debounced state (for helper text / draft)
  const debouncedSetCampaignName = useMemo(() => debounce((v) => setCampaignName(v), 60), []);
  const debouncedSetSubject = useMemo(() => debounce((v) => setSubject(v), 60), []);
  const debouncedSetBody = useMemo(() => debounce((v) => setBody(v), 60), []);
  const debouncedSetScheduleDate = useMemo(() => debounce((v) => setScheduleDate(v), 60), []);

  // Persist draft
  const persistDraft = useMemo(() => debounce((state) => {
    try { localStorage.setItem('cm_draft', JSON.stringify(state)); } catch { /* ignore */ }
  }, 150), []);

  useEffect(() => {
    const { campaign: c, subj, subjB, bodyText, sched } = getCurrentValues();
    persistDraft({ campaignName: c, subject: subj, subjectB: subjB, splitPercent, body: bodyText, scheduleDate: sched, selectedContacts });
  }, [getCurrentValues, splitPercent, selectedContacts, persistDraft]);

  // Restore draft
  useEffect(() => {
    try {
      const raw = localStorage.getItem('cm_draft');
      if (!raw) return;
      const d = JSON.parse(raw);
      setCampaignName(d.campaignName ?? '');
      setSubject(d.subject ?? '');
      setSubjectB(d.subjectB ?? '');
      setSplitPercent(Number.isFinite(d.splitPercent) ? d.splitPercent : 20);
      setBody(d.body ?? '');
      setScheduleDate(d.scheduleDate ?? '');
      setSelectedContacts(Array.isArray(d.selectedContacts) ? d.selectedContacts : []);
      infoToast('Draft restored');
      if (campaignNameRef.current) campaignNameRef.current.value = d.campaignName ?? '';
      if (subjectRef.current) subjectRef.current.value = d.subject ?? '';
      if (bodyRef.current) bodyRef.current.value = d.body ?? '';
      if (scheduleRef.current) scheduleRef.current.value = d.scheduleDate ?? '';
    } catch { /* ignore */ }
  }, []);

  // Fetch contacts
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
  }, [page, contactsPerPage, startTransition]);

  // Visible contacts (search + Hide DNC)
  const visibleContacts = useMemo(() => {
    let list = deferredContacts || [];
    if (hideDnc) list = list.filter(c => !c.dnc);
    if (query.trim()) {
      const q = query.toLowerCase();
      list = list.filter(c =>
        (c.name?.toLowerCase().includes(q)) || (c.email?.toLowerCase().includes(q))
      );
    }
    return list;
  }, [deferredContacts, hideDnc, query]);

  // WebSockets
  useEffect(() => {
    let retry = 0;
    const maxRetries = 5;
    let reconnectTimeout;

    const connectContactsWs = () => {
      try {
        ws.current = new WebSocket(toWsURL(cfg.contacts, '/ws/contacts'));
        ws.current.onopen = () => { infoToast('Contacts WS connected'); fetchContacts(); };
        ws.current.onmessage = (event) => {
          try {
            const { event: evt, data } = JSON.parse(event.data);
            startTransition(() => {
              if (evt === 'contacts_added') {
                infoToast(`Added ${data.created_count} contacts`);
                fetchContacts();
                setOutcomes(o => [{ ts: Date.now(), level: 'info', msg: `Contacts added: ${data.created_count}` }, ...o].slice(0, 200));
              } else if (evt === 'contact_updated') {
                setContacts((prev) => prev.map((c) => (c.id === data.id ? { ...c, ...data } : c)));
                setOutcomes(o => [{ ts: Date.now(), level: 'info', msg: `Contact updated: ${data.id}` }, ...o].slice(0, 200));
              } else if (evt === 'contact_deleted') {
                setContacts((prev) => prev.filter((c) => c.id !== data.id));
                setTotalContacts((t) => Math.max(0, t - 1));
                setOutcomes(o => [{ ts: Date.now(), level: 'warn', msg: `Contact deleted: ${data.id}` }, ...o].slice(0, 200));
              }
            });
          } catch (e) {
            errToast(`WS parse error: ${e.message}`);
          }
        };
        ws.current.onclose = () => {
          if (retry < maxRetries) {
            retry++;
            reconnectTimeout = setTimeout(connectContactsWs, 500 * retry);
          } else {
            toast.warn('WS failed, falling back to HTTP', { autoClose: 600 });
            fetchContacts();
          }
        };
        ws.current.onerror = () => ws.current && ws.current.close();
      } catch { /* ignore */ }
    };

    const connectOutcomesWs = () => {
      try {
        outcomesWs.current = new WebSocket(toWsURL(cfg.campaigns, '/ws/campaigns'));
        outcomesWs.current.onopen = () => {
          setOutcomes(o => [{ ts: Date.now(), level: 'info', msg: 'Campaigns WS connected' }, ...o].slice(0, 200));
        };
        outcomesWs.current.onmessage = (event) => {
          try {
            const payload = JSON.parse(event.data);
            const line = `[${payload.event}] ${payload.data?.campaign_name || ''} ${payload.data?.detail || ''}`.trim();
            setOutcomes(o => [{ ts: Date.now(), level: payload.event === 'failed' ? 'error' : 'info', msg: line }, ...o].slice(0, 200));
          } catch { /* ignore */ }
        };
      } catch { /* ignore */ }
    };

    connectContactsWs();
    fetchContacts();
    connectOutcomesWs();

    return () => {
      if (ws.current) ws.current.close();
      if (outcomesWs.current) outcomesWs.current.close();
      clearTimeout(reconnectTimeout);
    };
  }, [fetchContacts, startTransition]);

  // Selection handlers
  const handleSelectAllClick = useCallback(
    (e) => {
      startTransition(() => {
        const base = visibleContacts || deferredContacts;
        if (e.target.checked && base) {
          const ids = base.filter((c) => !c.dnc).map((n) => n.id);
          setSelectedContacts(ids);
          return;
        }
        setSelectedContacts([]);
      });
    },
    [visibleContacts, deferredContacts, startTransition],
  );

  const handleCheckboxClick = useCallback(
    (_e, id) => {
      startTransition(() => {
        setSelectedContacts((prev) => (prev.includes(id) ? prev.filter((x) => x !== id) : [...prev, id]));
      });
    },
    [startTransition],
  );

  const handleDncToggle = useCallback(
    async (contactId) => {
      try {
        await axios.put(`${cfg.contacts}/contacts/${contactId}/dnc`);
        startTransition(() => {
          setContacts((prev) =>
            prev.map((c) => (c.id === contactId ? { ...c, dnc: !c.dnc } : c)),
          );
        });
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
    [startTransition],
  );

  // Template render
  const renderTemplate = useCallback((tpl, contact) => {
    if (!tpl || !contact) return tpl || '';
    return tpl
      .replaceAll('{{ contact.name }}', contact.name ?? '')
      .replaceAll('{{ contact.email }}', contact.email ?? '')
      .replaceAll('{{ contact.city }}', contact.city ?? '')
      .replaceAll('{{ contact.state }}', contact.state ?? '')
      .replaceAll('{{ contact.company }}', contact.company ?? '');
  }, []);

  // Unsubscribe helper
  const quickAppendUnsub = useCallback(() => {
    const footer = `\n\n—\nIf you no longer wish to receive these emails, reply "UNSUBSCRIBE" or click {{ unsubscribe }}.`;
    if (bodyRef.current) {
      const el = bodyRef.current;
      el.value = (el.value || '') + footer;
      el.dispatchEvent(new Event('input', { bubbles: true }));
      okToast('Unsubscribe footer added');
    }
  }, []);

  // Token insertion
  const insertAtCursor = (el, text) => {
    if (!el) return;
    const start = el.selectionStart ?? el.value.length;
    const end = el.selectionEnd ?? el.value.length;
    const before = el.value.substring(0, start);
    const after = el.value.substring(end);
    el.value = before + text + after;
    const pos = start + text.length;
    el.setSelectionRange(pos, pos);
    el.focus();
    el.dispatchEvent(new Event('input', { bubbles: true }));
  };

  const insertToken = useCallback((token, target) => {
    const where = target || activeFieldRef.current;
    if (where === 'subject') insertAtCursor(subjectRef.current, token);
    else insertAtCursor(bodyRef.current, token);
  }, []);

  // Preflight (RELAXED): require ANY personalization token in subject OR body
  const runPreflight = useCallback(() => {
    const { campaign: c, subj, bodyText } = getCurrentValues();
    const issues = [];

    if (!c.trim()) issues.push({ key: 'name', msg: 'Campaign name missing' });
    if (!subj.trim()) issues.push({ key: 'subject', msg: 'Subject is empty' });
    if (subj.length > 120) issues.push({ key: 'subject_len', msg: 'Subject > 120 chars' });
    if (!bodyText.trim()) issues.push({ key: 'body', msg: 'Body is empty' });

    // personalization: accept ANY token (name/email/city/state/company) in EITHER subject or body
    if (!(hasAnyToken(bodyText) || hasAnyToken(subj))) {
      issues.push({
        key: 'token',
        msg: 'Add at least one personalization token (name/email/city/state/company) in subject or body',
        fix: () => insertToken('{{ contact.name }}', 'body'),
      });
    }

    // unsubscribe hint still encouraged
    if (!/unsubscribe/i.test(bodyText) && !/{{\s*unsubscribe\s*}}/i.test(bodyText)) {
      issues.push({
        key: 'unsub',
        msg: 'No unsubscribe hint found',
        fix: () => quickAppendUnsub(),
      });
    }
    if (selectedContacts.length === 0) {
      issues.push({ key: 'audience', msg: 'No contacts selected' });
    }
    return issues;
  }, [getCurrentValues, selectedContacts.length, insertToken, quickAppendUnsub]);

  // Actions
  const handleSubmit = useCallback(
    async () => {
      const { campaign: c, subj, subjB, bodyText } = getCurrentValues();
      const issues = runPreflight();
      if (issues.length) {
        const top = issues[0];
        errToast(`Preflight: ${top.msg}`, { onClick: () => top.fix && top.fix() });
        setMessage('Fix preflight issues before sending');
        setIsError(true);
        return;
      }

      setLoading(true);
      setMessage('');
      setIsError(false);
      infoToast('Sending campaign...');
      try {
        await axios.post(`${cfg.campaigns}/campaigns/send-now`, {
          name: c,
          subject: subj,
          subject_b: subjB || undefined,
          split_percent: subjB ? splitPercent : undefined,
          body_template: bodyText,
          contact_ids: selectedContacts,
        });
        okToast('Campaign queued to send now');
        setOutcomes(o => [{ ts: Date.now(), level: 'info', msg: `Queued campaign "${c}" to ${selectedContacts.length} contacts` }, ...o].slice(0, 200));
      } catch (error) {
        const msg = error.response?.data?.detail || error.message;
        setMessage(`Send error: ${msg}`);
        setIsError(true);
        setOutcomes(o => [{ ts: Date.now(), level: 'error', msg }, ...o].slice(0, 200));
        errToast(msg);
      } finally {
        setLoading(false);
      }
    },
    [getCurrentValues, splitPercent, selectedContacts, runPreflight],
  );

  const handleSchedule = useCallback(
    async () => {
      const { campaign: c, subj, subjB, bodyText, sched } = getCurrentValues();
      const issues = runPreflight();
      if (!sched) issues.push({ key: 'schedule', msg: 'Schedule date missing' });

      if (issues.length) {
        const top = issues[0];
        errToast(`Preflight: ${top.msg}`, { onClick: () => top.fix && top.fix() });
        setMessage('Fix preflight issues before scheduling');
        setIsError(true);
        return;
      }

      setLoading(true);
      setMessage('');
      setIsError(false);
      infoToast('Scheduling...');
      try {
        await axios.post(`${cfg.campaigns}/campaigns/schedule`, {
          name: c,
          subject: subj,
          subject_b: subjB || undefined,
          split_percent: subjB ? splitPercent : undefined,
          body_template: bodyText,
          contact_ids: selectedContacts,
          scheduled_at: new Date(sched).toISOString(),
        });
        okToast(`Scheduled for ${new Date(sched).toLocaleString()}`);
        setOutcomes(o => [{ ts: Date.now(), level: 'info', msg: `Scheduled "${c}" at ${new Date(sched).toLocaleString()}` }, ...o].slice(0, 200));
        // Reset
        setCampaignName(''); setSubject(''); setSubjectB(''); setSplitPercent(20); setBody(''); setSelectedContacts([]); setScheduleDate('');
        if (campaignNameRef.current) campaignNameRef.current.value = '';
        if (subjectRef.current) subjectRef.current.value = '';
        if (bodyRef.current) bodyRef.current.value = '';
        if (scheduleRef.current) scheduleRef.current.value = '';
      } catch (error) {
        const msg = error.response?.data?.detail || error.message;
        setMessage(`Schedule error: ${msg}`);
        setIsError(true);
        setOutcomes(o => [{ ts: Date.now(), level: 'error', msg }, ...o].slice(0, 200));
        errToast(msg);
      } finally {
        setLoading(false);
      }
    },
    [getCurrentValues, splitPercent, selectedContacts, runPreflight],
  );

  // Table rendering
  const isSelected = useCallback((id) => selectedContacts.includes(id), [selectedContacts]);

  const totalPages = useMemo(
    () => (deferredContacts && totalContacts ? Math.ceil(totalContacts / contactsPerPage) : 1),
    [deferredContacts, totalContacts, contactsPerPage],
  );

  const listForTable = useMemo(
    () => (Array.isArray(visibleContacts) ? visibleContacts : deferredContacts),
    [visibleContacts, deferredContacts]
  );

  const tableRows = useMemo(() => {
    if (loading || !listForTable || listForTable.length === 0) {
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
          {!loading && listForTable && listForTable.length === 0 && (
            <TableRow>
              <TableCell colSpan={4} sx={{ color: '#ff8585', textAlign: 'center' }}>
                No contacts available. Please ingest or retry.
              </TableCell>
            </TableRow>
          )}
        </>
      );
    }
    return listForTable.map((row, index) => {
      const selected = isSelected(row.id);
      const rowStyle = row.dnc
        ? { backgroundColor: 'rgba(255, 107, 107, 0.12)', opacity: 0.8 }
        : { backgroundColor: index % 2 === 0 ? 'rgba(255,255,255,0.02)' : 'rgba(255,255,255,0.04)' };
      return (
        <TableRow hover key={row.id} sx={rowStyle}>
          <TableCell padding="checkbox">
            <Checkbox
              checked={selected}
              disabled={row.dnc}
              onClick={(e) => handleCheckboxClick(e, row.id)}
              inputProps={{ 'aria-label': `Select contact ${row.name}` }}
            />
          </TableCell>
          <TableCell sx={{ color: '#fff' }}>{row.name}</TableCell>
          <TableCell sx={{ color: '#fff' }}>{row.email}</TableCell>
          <TableCell>
            <Box display="flex" gap={1}>
              <Button
                size="small"
                variant="outlined"
                color={row.dnc ? 'success' : 'error'}
                onClick={() => handleDncToggle(row.id)}
                aria-label={row.dnc ? `Allow contact ${row.name}` : `Mark ${row.name} as DNC`}
              >
                {row.dnc ? 'Allow' : 'DNC'}
              </Button>
              <Button
                size="small"
                variant="text"
                onClick={() => { setPreviewContact(row); setPreviewOpen(true); }}
              >
                Preview
              </Button>
            </Box>
          </TableCell>
        </TableRow>
      );
    });
  }, [listForTable, loading, isSelected, handleCheckboxClick, handleDncToggle]);

  // Keyboard shortcuts
  useEffect(() => {
    const onKey = (e) => {
      if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === 's') {
        e.preventDefault();
        if (!loading) handleSchedule();
      } else if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
        e.preventDefault();
        if (!loading) handleSubmit();
      } else if (!e.ctrlKey && !e.metaKey && e.key === '/' && document.activeElement?.tagName !== 'INPUT' && document.activeElement?.tagName !== 'TEXTAREA') {
        e.preventDefault();
        searchRef.current?.focus();
      } else if (e.altKey && (e.key.toLowerCase() === 'p')) {
        e.preventDefault();
        const first = (visibleContacts || []).find(c => selectedContacts.includes(c.id));
        if (first) { setPreviewContact(first); setPreviewOpen(true); }
      }
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [handleSchedule, handleSubmit, loading, visibleContacts, selectedContacts]);

  // Live snapshot for helper UI
  const { campaign: liveCampaign, subj: liveSubject, bodyText: liveBody, sched: liveSched } = getCurrentValues();
  const formValidNow = liveCampaign.trim() && liveSubject.trim() && liveBody.trim() && selectedContacts.length > 0;
  const tokensInBody = listTokensUsed(liveBody);
  const tokensInSubject = listTokensUsed(liveSubject);

  return (
    <PageContainer>
      {loading && <LinearProgress sx={{ mb: 2 }} />}
      <Grid container spacing={2}>
        <Grid item xs={12} md={7}>
          <Card elevation={0}>
            <Box display="flex" justifyContent="space-between" alignItems="center" mb={2}>
              <Typography variant="h6" sx={{ fontWeight: 700 }}>
                Select Contacts ({selectedContacts.length} / {visibleContacts ? visibleContacts.filter((c) => !c.dnc).length : 0})
              </Typography>
              <Box display="flex" gap={2} alignItems="center">
                <TextField
                  inputRef={searchRef}
                  size="small"
                  placeholder="Search name or email…"
                  onChange={(e) => setQuery(e.target.value)}
                  inputProps={{ 'aria-label': 'Search contacts' }}
                  sx={{ minWidth: 240 }}
                />
                <Box display="flex" alignItems="center" gap={1}>
                  <Checkbox
                    checked={hideDnc}
                    onChange={(e) => setHideDnc(e.target.checked)}
                    inputProps={{ 'aria-label': 'Hide DNC' }}
                  />
                  <Typography variant="body2">Hide DNC</Typography>
                </Box>

                <Select
                  value={contactsPerPage}
                  onChange={(e) => setContactsPerPage(Number(e.target.value))}
                  sx={{ color: '#fff', bgcolor: 'rgba(255,255,255,0.05)' }}
                  aria-label="Contacts per page"
                >
                  {[25, 50, 100].map((n) => (
                    <MenuItem key={n} value={n}>
                      {n}
                    </MenuItem>
                  ))}
                </Select>
                <Button onClick={fetchContacts} disabled={loading} aria-label="Retry fetch">
                  Retry
                </Button>
                <Button
                  onClick={() => { localStorage.removeItem('cm_draft'); okToast('Draft cleared'); }}
                  disabled={loading}
                >
                  Clear Draft
                </Button>
              </Box>
            </Box>
            <TableContainer>
              <Table size="small">
                <TableHead>
                  <TableRow>
                    <TableCell padding="checkbox" sx={{ bgcolor: 'rgba(255,255,255,0.06)' }}>
                      <Checkbox
                        indeterminate={
                          visibleContacts &&
                          selectedContacts.length > 0 &&
                          selectedContacts.length < visibleContacts.filter((c) => !c.dnc).length
                        }
                        checked={
                          visibleContacts &&
                          visibleContacts.length > 0 &&
                          selectedContacts.length === visibleContacts.filter((c) => !c.dnc).length
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
              Compose campaign
            </Typography>

            {/* Token palette */}
            <Box sx={{ display: 'flex', gap: 1, flexWrap: 'wrap', mb: 1 }}>
              {['{{ contact.name }}','{{ contact.email }}','{{ contact.company }}','{{ contact.city }}','{{ contact.state }}','{{ unsubscribe }}'].map(tok => (
                <Button
                  key={tok}
                  size="small"
                  variant="outlined"
                  onClick={() => insertToken(tok)}
                  aria-label={`Insert token ${tok}`}
                  onMouseDown={(e) => e.preventDefault()}
                >
                  {tok}
                </Button>
              ))}
              <Button size="small" onClick={() => { activeFieldRef.current = 'subject'; }} variant="text">Target: Subject</Button>
              <Button size="small" onClick={() => { activeFieldRef.current = 'body'; }} variant="text">Target: Body</Button>
            </Box>

            <TextField
              label="Campaign Name"
              inputRef={campaignNameRef}
              onChange={(e) => debouncedSetCampaignName(e.target.value)}
              fullWidth
              margin="normal"
              error={!liveCampaign.trim()}
              helperText={!liveCampaign.trim() ? 'Campaign name is required' : ''}
              inputProps={{ 'aria-label': 'Campaign name' }}
            />

            <TextField
              inputRef={subjectRef}
              onFocus={() => (activeFieldRef.current = 'subject')}
              label="Email Subject"
              onChange={(e) => debouncedSetSubject(e.target.value)}
              fullWidth
              margin="normal"
              error={!liveSubject.trim() || liveSubject.length > 120}
              helperText={
                !liveSubject.trim()
                  ? 'Subject is required'
                  : (liveSubject.length > 120 ? 'Subject too long' : (tokensInSubject.length ? `Tokens: ${tokensInSubject.join(', ')}` : 'Tip: add a token for better open rates'))
              }
              inputProps={{ 'aria-label': 'Email subject' }}
              placeholder="e.g., Hi {{ contact.name }}, quick question"
            />

            {/* A/B Subject */}
            <TextField
              label="Subject B (A/B Test)"
              onChange={(e) => setSubjectB(e.target.value)}
              fullWidth
              margin="normal"
              placeholder="Optional"
            />
            <Box display="flex" alignItems="center" gap={2}>
              <Typography variant="body2" sx={{ opacity: 0.8 }}>A/B Split % (send B to % of audience)</Typography>
              <Select
                size="small"
                value={splitPercent}
                onChange={(e) => setSplitPercent(Number(e.target.value))}
                sx={{ color: '#fff', bgcolor: 'rgba(255,255,255,0.05)' }}
              >
                {[10,20,30,40,50].map(n => <MenuItem key={n} value={n}>{n}%</MenuItem>)}
              </Select>
            </Box>

            <TextField
              inputRef={bodyRef}
              onFocus={() => (activeFieldRef.current = 'body')}
              label="Email Body (personalize with tokens)"
              multiline
              rows={4}
              onChange={(e) => debouncedSetBody(e.target.value)}
              fullWidth
              margin="normal"
              error={!liveBody.trim()}
              helperText={
                !liveBody.trim()
                  ? 'Email body is required'
                  : (tokensInBody.length ? `Tokens: ${tokensInBody.join(', ')}` : 'Tip: add {{ contact.name }} or any token above')
              }
              inputProps={{ 'aria-label': 'Email body' }}
            />

            <TextField
              label="Schedule For (Optional)"
              type="datetime-local"
              inputRef={scheduleRef}
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
                  disabled={loading || !formValidNow}
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
                  disabled={loading || !formValidNow || !liveSched}
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

      {/* Outcomes console */}
      <Card elevation={0} style={{ marginTop: 16 }}>
        <Box display="flex" justifyContent="space-between" alignItems="center" mb={1}>
          <Typography variant="subtitle1" sx={{ fontWeight: 700 }}>Outcomes</Typography>
          <Box display="flex" gap={1}>
            <Button size="small" onClick={() => setOutcomes([])}>Clear</Button>
            <Button size="small" onClick={() => setOutcomes(o => [{ ts: Date.now(), level: 'info', msg: 'Ping' }, ...o].slice(0,200))}>Ping</Button>
          </Box>
        </Box>
        <Box sx={{ maxHeight: 200, overflowY: 'auto', fontFamily: 'ui-monospace, SFMono-Regular, Menlo, monospace', fontSize: 12, p: 1, bgcolor: 'rgba(255,255,255,0.03)', borderRadius: 1 }}>
          {(outcomes || []).map((l, i) => (
            <Box key={i} sx={{ opacity: 0.95, mb: 0.5 }}>
              <span style={{ opacity: 0.6 }}>{new Date(l.ts).toLocaleTimeString()}</span>{' '}
              <strong style={{ textTransform: 'uppercase', color: l.level === 'error' ? '#ff6b6b' : l.level === 'warn' ? '#ffb86b' : '#a0e7a0' }}>
                {l.level}
              </strong>{' '}
              <span>{l.msg}</span>
            </Box>
          ))}
          {(!outcomes || outcomes.length === 0) && <Typography variant="body2" sx={{ opacity: 0.6 }}>No events yet.</Typography>}
        </Box>
      </Card>

      {/* Preview Drawer */}
      {previewOpen && (
        <Box
          sx={{
            position: 'fixed', top: 0, right: 0, height: '100vh', width: { xs: '100%', md: 520 },
            bgcolor: '#111827', boxShadow: 24, p: 3, zIndex: 1600, overflowY: 'auto'
          }}
          role="dialog" aria-label="Preview email"
        >
          <Box display="flex" justifyContent="space-between" alignItems="center" mb={2}>
            <Typography variant="h6">Preview for {previewContact?.name}</Typography>
            <Button onClick={() => setPreviewOpen(false)}>Close</Button>
          </Box>
          <Typography variant="subtitle2" sx={{ opacity: 0.7, mb: 1 }}>Subject</Typography>
          <Box sx={{ bgcolor: 'rgba(255,255,255,0.04)', p: 1.5, borderRadius: 1, mb: 2 }}>
            {renderTemplate(subjectRef.current?.value ?? subject, previewContact)}
          </Box>
          <Typography variant="subtitle2" sx={{ opacity: 0.7, mb: 1 }}>Body</Typography>
          <Box sx={{ bgcolor: 'rgba(255,255,255,0.04)', p: 1.5, borderRadius: 1, whiteSpace: 'pre-wrap' }}>
            {renderTemplate(bodyRef.current?.value ?? body, previewContact)}
          </Box>
        </Box>
      )}

      {message && <Alert severity={isError ? 'error' : 'success'} sx={{ mt: 2 }}>{message}</Alert>}
    </PageContainer>
  );
});

export default CampaignManager;
