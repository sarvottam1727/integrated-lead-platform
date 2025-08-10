import React, { useState, useEffect, useTransition, useDeferredValue, useCallback } from 'react';
import axios from 'axios';
import { Table, TableHead, TableRow, TableCell, TableBody, Alert, LinearProgress } from '@mui/material';
import { toast } from 'react-toastify';
import { Card, PageContainer, SkeletonRow } from './Common';

const cfg = {
  API_KEY: process.env.REACT_APP_API_KEY || 'your-super-secret-key-12345',
  orchestrator: process.env.REACT_APP_ORCH_URL || 'http://localhost:8006',
  contacts: process.env.REACT_APP_CONTACTS_URL || 'http://localhost:8002',
  campaigns: process.env.REACT_APP_CAMPAIGNS_URL || 'http://localhost:8005',
  connector: process.env.REACT_APP_CONNECTOR_URL || 'http://localhost:8011',
};

const okToast = (msg) => toast.success(msg, { autoClose: 500 });
const errToast = (msg) => toast.error(msg, { autoClose: 900 });

const CampaignHistory = React.memo(function CampaignHistory() {
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

export default CampaignHistory;
