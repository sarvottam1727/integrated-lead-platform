import React, { useState } from 'react';
import axios from 'axios';
import { Box, Button, TextField, Typography } from '@mui/material';
import { Card } from './Common';

const cfg = {
  API_KEY: process.env.REACT_APP_API_KEY || 'your-super-secret-key-12345',
  email: process.env.REACT_APP_EMAIL_URL || 'http://localhost:8001',
};

const AIWriter = React.memo(function AIWriter() {
  const [prompt, setPrompt] = useState('');
  const [contact, setContact] = useState('{}');
  const [suggestion, setSuggestion] = useState('');
  const [loading, setLoading] = useState(false);

  const generate = async () => {
    setLoading(true);
    try {
      const { data } = await axios.post(
        `${cfg.email}/generate-copy`,
        { prompt, contact: JSON.parse(contact || '{}') },
        { headers: { 'X-API-Key': cfg.API_KEY } }
      );
      setSuggestion(data.text || '');
    } catch (err) {
      setSuggestion(err.response?.data?.detail || err.message);
    } finally {
      setLoading(false);
    }
  };

  return (
    <Card sx={{ p: 2 }}>
      <Typography variant="h6" gutterBottom>
        AI Writer
      </Typography>
      <TextField
        label="Prompt"
        multiline
        minRows={2}
        fullWidth
        value={prompt}
        onChange={(e) => setPrompt(e.target.value)}
        sx={{ mb: 2 }}
      />
      <TextField
        label="Contact JSON"
        multiline
        minRows={2}
        fullWidth
        value={contact}
        onChange={(e) => setContact(e.target.value)}
        sx={{ mb: 2 }}
      />
      <Button variant="contained" onClick={generate} disabled={loading}>
        Generate
      </Button>
      {suggestion && (
        <TextField
          label="Suggestion"
          multiline
          minRows={4}
          fullWidth
          value={suggestion}
          InputProps={{ readOnly: true }}
          sx={{ mt: 2 }}
        />
      )}
    </Card>
  );
});

export default AIWriter;
