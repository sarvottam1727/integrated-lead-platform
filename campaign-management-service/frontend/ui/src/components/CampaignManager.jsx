import React, { useState, useEffect, useRef, useCallback, useMemo, useTransition, useDeferredValue } from 'react';
import axios from 'axios';
import { Button, TextField, Checkbox, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, Grid, Typography, Box, LinearProgress, MenuItem, Select, Alert, CircularProgress } from '@mui/material';
import { toast } from 'react-toastify';
import debounce from 'lodash.debounce';
import { Card, PageContainer, SkeletonRow } from './Common';

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
        const contact = contacts.find((c) => c.id === contactId);
        const action = contact.dnc ? 'allow' : 'dnc';
        await axios.post(`${cfg.contacts}/contacts/${contactId}/${action}`);
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
    [contacts],
  );

  const handleSubmit = useCallback(
    async () => {
      if (!validateForm()) {
        setMessage('Missing fields');
        setIsError(true);
        return errToast('Missing fields');
      }
      setLoading(true);
      setMessage('');
      setIsError(false);
      infoToast('Sending campaign...');
      try {
        await axios.post(`${cfg.campaigns}/campaigns/send`, {
          name: campaignName,
          subject,
          body,
          contacts: selectedContacts,
        });
        okToast('Campaign sent');
      } catch (error) {
        const msg = error.response?.data?.detail || error.message;
        setMessage(`Send error: ${msg}`);
        setIsError(true);
        errToast(msg);
      } finally {
        setLoading(false);
      }
    },
    [campaignName, subject, body, selectedContacts, validateForm],
  );

  const handleSchedule = useCallback(
    async () => {
      if (!validateForm() || !scheduleDate) {
        setMessage('Missing schedule info');
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
          body,
          contacts: selectedContacts,
          schedule_date: scheduleDate,
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
    },
    [campaignName, subject, body, selectedContacts, scheduleDate, validateForm],
  );

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
                  {[25, 50, 100].map((n) => (
                    <MenuItem key={n} value={n}>
                      {n}
                    </MenuItem>
                  ))}
                </Select>
                <Button onClick={handleRetryFetch} disabled={loading} aria-label="Retry fetch">
                  Retry
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

export default CampaignManager;
