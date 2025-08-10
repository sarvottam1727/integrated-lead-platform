import React, { Suspense, lazy } from 'react';
import { BrowserRouter as Router, Routes, Route, NavLink, useLocation } from 'react-router-dom';
import axios from 'axios';
import {
  FaUpload,
  FaBullhorn,
  FaCog,
  FaHistory,
  FaExclamationCircle,
} from 'react-icons/fa';
import { Button, Typography, LinearProgress, ThemeProvider, createTheme } from '@mui/material';
import { styled } from '@mui/material/styles';
import { keyframes } from '@emotion/react';
import { motion, AnimatePresence, useMotionValue, useTransform } from 'framer-motion';
import { ToastContainer } from 'react-toastify';
import { Card } from './components/Common';
import 'react-toastify/dist/ReactToastify.css';
import '@xterm/xterm/css/xterm.css';

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
        <IconWrap>{React.isValidElement(icon) ? icon : null}</IconWrap>
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

// Lazy-loaded Components
const LazyDevPanel = lazy(() => import('./components/DevPanel'));
const LazyFileUpload = lazy(() =>
  import('./components/FileUpload').then(m => ({ default: m.default || m.FileUpload }))
);
const LazyCampaignManager = lazy(() => import('./components/CampaignManager'));
const LazyCampaignHistory = lazy(() => import('./components/CampaignHistory'));

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
