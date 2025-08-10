import React from 'react';
import { styled } from '@mui/material/styles';
import { Paper, TableRow } from '@mui/material';
import { keyframes } from '@emotion/react';
import { motion } from 'framer-motion';

/* ===== animations ===== */
const shimmer = keyframes`
  0% { background-position: 200% 0; }
  100% { background-position: -200% 0; }
`;
const rotate = keyframes`
  to { transform: rotate(360deg); }
`;

/* ===== Card (motion-ready, glow, tone) ===== */
export const Card = styled(motion(Paper), {
  shouldForwardProp: (prop) =>
    !['glow', 'tone', 'interactive', 'dense', 'hoverLift'].includes(prop),
})(({ theme, glow = false, tone = 'primary', interactive = false, dense = false, hoverLift = true }) => {
  const { palette } = theme;
  const tones = {
    primary: palette.primary?.main || '#60a5fa',
    info: palette.info?.main || '#38bdf8',
    success: palette.success?.main || '#22c55e',
    warning: palette.warning?.main || '#f59e0b',
    error: palette.error?.main || '#ef4444',
    neutral: 'rgba(255,255,255,0.7)',
  };
  const accent = tones[tone] || tones.primary;

  return {
    '--card-accent': accent,
    padding: dense ? 16 : 24,
    background:
      'radial-gradient(1200px 800px at -10% -30%, rgba(96,165,250,0.08), transparent 40%),' +
      'radial-gradient(800px 400px at 110% 130%, rgba(59,130,246,0.08), transparent 40%),' +
      'rgba(255,255,255,0.05)',
    borderRadius: 16,
    border: '1px solid rgba(255,255,255,0.08)',
    boxShadow: '0 20px 60px rgba(0,0,0,0.35), inset 0 1px 0 rgba(255,255,255,0.05)',
    transform: 'translateZ(0)',
    position: 'relative',
    overflow: 'hidden',

    ...(glow && {
      /* animated conic ring (masked) */
      '&::before': {
        content: '""',
        position: 'absolute',
        inset: -1,
        borderRadius: 16,
        padding: 1,
        background:
          'conic-gradient(from 0deg at 50% 50%,' +
          'transparent 0deg, var(--card-accent) 90deg, transparent 180deg,' +
          'var(--card-accent) 270deg, transparent 360deg)',
        WebkitMask: 'linear-gradient(#000 0 0) content-box, linear-gradient(#000 0 0)',
        WebkitMaskComposite: 'xor',
        maskComposite: 'exclude',
        animation: `${rotate} 10s linear infinite`,
        pointerEvents: 'none',
        opacity: 0.8,
      },
    }),

    ...(interactive && {
      cursor: 'pointer',
      transition: 'transform 120ms ease, box-shadow 120ms ease, border-color 120ms ease',
      ...(hoverLift ? { '&:hover': { transform: 'translateY(-2px)' } } : null),
      '&:active': { transform: 'translateY(0) scale(0.998)' },
      '&:focus-visible': { outline: `2px solid ${accent}`, outlineOffset: 2 },
    }),

    '@media (prefers-reduced-motion: reduce)': {
      transition: 'none',
      '&::before': { animationDuration: '0s' },
    },
  };
});

/* ===== Page container (grid/bleed options) ===== */
const StyledPage = styled(motion.div, {
  shouldForwardProp: (prop) => !['grid', 'bleed'].includes(prop),
})(({ grid = false, bleed = false }) => ({
  padding: 32,
  margin: bleed ? 0 : 24,
  background: 'rgba(18,18,30,0.6)',
  border: '1px solid rgba(255,255,255,0.06)',
  borderRadius: 16,
  boxShadow: '0 12px 40px rgba(0,0,0,0.35)',
  backdropFilter: 'blur(10px)',
  position: 'relative',
  overflow: 'hidden',

  /* optional subtle grid overlay */
  ...(grid && {
    '&::after': {
      content: '""',
      position: 'absolute',
      inset: 0,
      background:
        'repeating-linear-gradient(0deg, rgba(255,255,255,0.05) 0, rgba(255,255,255,0.05) 1px, transparent 1px, transparent 20px),' +
        'repeating-linear-gradient(90deg, rgba(255,255,255,0.05) 0, rgba(255,255,255,0.05) 1px, transparent 1px, transparent 20px)',
      opacity: 0.06,
      pointerEvents: 'none',
    },
  }),

  '@media (max-width: 600px)': {
    margin: bleed ? 0 : 12,
    padding: 16,
  },
  '@media (prefers-reduced-motion: reduce)': {
    transition: 'none',
  },
}));

export const PageContainer = (props) => (
  <StyledPage
    initial={{ opacity: 0, y: 6 }}
    animate={{ opacity: 1, y: 0 }}
    exit={{ opacity: 0, y: -6 }}
    transition={{ duration: 0.12 }}
    {...props}
  />
);

// Replace your SkeletonRow block with this:
export const SkeletonRow = styled(TableRow)(() => ({
  backgroundColor: 'rgba(255,255,255,0.03)',
  '& > td': {
    padding: 16,
    color: 'transparent',
    background: `linear-gradient(
      90deg,
      rgba(255,255,255,0.04) 25%,
      rgba(255,255,255,0.09) 50%,
      rgba(255,255,255,0.04) 75%
    )`,
    backgroundSize: '200% 100%',
    animation: `${shimmer} var(--sk-speed, 0.6s) infinite`,
  },
}));

