import React from 'react';
import { styled } from '@mui/material/styles';
import { Paper, TableRow } from '@mui/material';
import { keyframes } from '@emotion/react';
import { motion } from 'framer-motion';

export const Card = styled(Paper)`
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

export const PageContainer = (props) => (
  <StyledPage
    initial={{ opacity: 0, y: 6 }}
    animate={{ opacity: 1, y: 0 }}
    exit={{ opacity: 0, y: -6 }}
    transition={{ duration: 0.12 }}
    {...props}
  />
);

export const SkeletonRow = styled(TableRow)`
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
