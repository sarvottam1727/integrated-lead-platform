import React, { useState, useCallback, useTransition, useMemo } from 'react';
import axios from 'axios';
import { Button, Typography, Grid, Box, TextField, LinearProgress, Alert, CircularProgress } from '@mui/material';
import { motion } from 'framer-motion';
import { toast } from 'react-toastify';
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

const FileUpload = React.memo(function FileUpload() {
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

export default FileUpload;
