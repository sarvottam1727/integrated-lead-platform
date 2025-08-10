import { render, screen } from '@testing-library/react';
import App from './App';

test('renders the main application heading', () => {
  render(<App />);
  // This test now looks for the actual title of our application
  const headingElement = screen.getByText(/Sai Unisonic Campaign Manager/i);
  expect(headingElement).toBeInTheDocument();
});