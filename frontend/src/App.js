import React from 'react';
import { BrowserRouter as Router, Routes, Route, Link, useLocation } from 'react-router-dom';
import { useState, useEffect } from 'react';
import axios from 'axios';

const API_BASE = process.env.REACT_APP_API_URL || '/api';

// ─── API Helper ──────────────────────────────────────────────────────────────
const api = axios.create({ baseURL: API_BASE, timeout: 10000 });

// ─── Navigation ──────────────────────────────────────────────────────────────
function Sidebar() {
  const location = useLocation();
  const navItems = [
    { path: '/', label: '📊 Dashboard', icon: 'dashboard' },
    { path: '/portfolio', label: '💼 Portfolio', icon: 'portfolio' },
    { path: '/orders', label: '📋 Orders', icon: 'orders' },
    { path: '/market', label: '📈 Market Data', icon: 'market' },
    { path: '/traders', label: '👥 Traders', icon: 'traders' },
    { path: '/accounts', label: '💰 Accounts', icon: 'accounts' },
    { path: '/trades', label: '🔄 Trade History', icon: 'trades' },
    { path: '/settlements', label: '✅ Settlements', icon: 'settlements' },
  ];

  return (
    <nav style={styles.sidebar}>
      <div style={styles.logo}>
        <h2 style={{ margin: 0, color: '#00d4aa' }}>📈 TradeHub</h2>
        <p style={{ margin: '4px 0 0', fontSize: '12px', color: '#888' }}>Trading Platform</p>
      </div>
      {navItems.map(item => (
        <Link
          key={item.path}
          to={item.path}
          style={{
            ...styles.navItem,
            backgroundColor: location.pathname === item.path ? '#1e3a5f' : 'transparent',
            borderLeft: location.pathname === item.path ? '3px solid #00d4aa' : '3px solid transparent',
          }}
        >
          {item.label}
        </Link>
      ))}
    </nav>
  );
}

// ─── Dashboard Page ──────────────────────────────────────────────────────────
function Dashboard() {
  const [health, setHealth] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    api.get('/health').then(r => setHealth(r.data)).catch(console.error).finally(() => setLoading(false));
  }, []);

  if (loading) return <div style={styles.loading}>Loading dashboard...</div>;

  return (
    <div>
      <h1 style={styles.pageTitle}>Trading Dashboard</h1>
      <div style={styles.cardGrid}>
        <div style={{ ...styles.card, borderTop: '3px solid #00d4aa' }}>
          <h3>System Status</h3>
          <p style={{ fontSize: '24px', fontWeight: 'bold', color: health?.status === 'UP' ? '#00d4aa' : '#ff6b6b' }}>
            {health?.status || 'Unknown'}
          </p>
        </div>
        <div style={{ ...styles.card, borderTop: '3px solid #4dabf7' }}>
          <h3>API Gateway</h3>
          <p style={{ fontSize: '14px', color: '#aaa' }}>Port: {health?.port || 'N/A'}</p>
          <p style={{ fontSize: '12px', color: '#666' }}>{health?.timestamp}</p>
        </div>
        <div style={{ ...styles.card, borderTop: '3px solid #ffd43b' }}>
          <h3>Trading Engine</h3>
          <p style={{ fontSize: '14px', color: '#aaa' }}>Active</p>
          <p style={{ fontSize: '12px', color: '#666' }}>Order matching enabled</p>
        </div>
        <div style={{ ...styles.card, borderTop: '3px solid #ff6b6b' }}>
          <h3>Market Data</h3>
          <p style={{ fontSize: '14px', color: '#aaa' }}>WebSocket Streaming</p>
          <p style={{ fontSize: '12px', color: '#666' }}>Real-time quotes</p>
        </div>
      </div>

      {health?.services && (
        <div style={styles.card}>
          <h3>Service Health</h3>
          <table style={styles.table}>
            <thead>
              <tr><th>Service</th><th>Status</th></tr>
            </thead>
            <tbody>
              {Object.entries(health.services).map(([name, info]) => (
                <tr key={name}>
                  <td>{name}</td>
                  <td style={{ color: info.status === 'UP' ? '#00d4aa' : '#ff6b6b' }}>{info.status}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

// ─── Market Data Page ────────────────────────────────────────────────────────
function MarketData() {
  const [securities, setSecurities] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    api.get('/market-data/securities')
      .then(r => setSecurities(r.data?.securities || r.data || []))
      .catch(console.error)
      .finally(() => setLoading(false));
  }, []);

  if (loading) return <div style={styles.loading}>Loading market data...</div>;

  return (
    <div>
      <h1 style={styles.pageTitle}>Market Data</h1>
      <div style={styles.card}>
        <table style={styles.table}>
          <thead>
            <tr><th>Symbol</th><th>Name</th><th>Type</th><th>Price</th><th>Change</th></tr>
          </thead>
          <tbody>
            {securities.map(s => (
              <tr key={s.symbol}>
                <td style={{ fontWeight: 'bold', color: '#00d4aa' }}>{s.symbol}</td>
                <td>{s.name}</td>
                <td><span style={styles.badge}>{s.type}</span></td>
                <td>${Number(s.current_price || s.price || 0).toLocaleString('en-US', { minimumFractionDigits: 2 })}</td>
                <td style={{ color: Math.random() > 0.5 ? '#00d4aa' : '#ff6b6b' }}>
                  {(Math.random() * 5 - 2.5).toFixed(2)}%
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

// ─── Portfolio Page ──────────────────────────────────────────────────────────
function Portfolio() {
  return (
    <div>
      <h1 style={styles.pageTitle}>Portfolio</h1>
      <div style={styles.cardGrid}>
        <div style={{ ...styles.card, borderTop: '3px solid #00d4aa' }}>
          <h3>Total Value</h3>
          <p style={{ fontSize: '28px', fontWeight: 'bold', color: '#00d4aa' }}>$245,892.50</p>
        </div>
        <div style={{ ...styles.card, borderTop: '3px solid #4dabf7' }}>
          <h3>Day's P&L</h3>
          <p style={{ fontSize: '28px', fontWeight: 'bold', color: '#00d4aa' }}>+$1,234.56</p>
        </div>
        <div style={{ ...styles.card, borderTop: '3px solid #ffd43b' }}>
          <h3>Buying Power</h3>
          <p style={{ fontSize: '28px', fontWeight: 'bold', color: '#fff' }}>$50,000.00</p>
        </div>
      </div>
      <div style={styles.card}>
        <h3>Holdings</h3>
        <p style={{ color: '#888' }}>Connect to trading account to view holdings</p>
      </div>
    </div>
  );
}

// ─── Orders Page ─────────────────────────────────────────────────────────────
function Orders() {
  const [orderType, setOrderType] = useState('market');
  const [side, setSide] = useState('buy');

  return (
    <div>
      <h1 style={styles.pageTitle}>Order Management</h1>
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 2fr', gap: '20px' }}>
        <div style={styles.card}>
          <h3>Place Order</h3>
          <div style={{ marginBottom: '12px' }}>
            <label style={styles.label}>Side</label>
            <div style={{ display: 'flex', gap: '8px' }}>
              <button onClick={() => setSide('buy')} style={{ ...styles.btn, backgroundColor: side === 'buy' ? '#00d4aa' : '#333' }}>Buy</button>
              <button onClick={() => setSide('sell')} style={{ ...styles.btn, backgroundColor: side === 'sell' ? '#ff6b6b' : '#333' }}>Sell</button>
            </div>
          </div>
          <div style={{ marginBottom: '12px' }}>
            <label style={styles.label}>Symbol</label>
            <input style={styles.input} placeholder="AAPL" />
          </div>
          <div style={{ marginBottom: '12px' }}>
            <label style={styles.label}>Order Type</label>
            <select style={styles.input} value={orderType} onChange={e => setOrderType(e.target.value)}>
              <option value="market">Market</option>
              <option value="limit">Limit</option>
              <option value="stop">Stop</option>
              <option value="stop_limit">Stop-Limit</option>
            </select>
          </div>
          <div style={{ marginBottom: '12px' }}>
            <label style={styles.label}>Quantity</label>
            <input style={styles.input} type="number" placeholder="100" />
          </div>
          {orderType !== 'market' && (
            <div style={{ marginBottom: '12px' }}>
              <label style={styles.label}>Price</label>
              <input style={styles.input} type="number" step="0.01" placeholder="175.50" />
            </div>
          )}
          <button style={{ ...styles.btn, backgroundColor: side === 'buy' ? '#00d4aa' : '#ff6b6b', width: '100%', padding: '12px' }}>
            {side === 'buy' ? 'Place Buy Order' : 'Place Sell Order'}
          </button>
        </div>
        <div style={styles.card}>
          <h3>Recent Orders</h3>
          <table style={styles.table}>
            <thead>
              <tr><th>Order ID</th><th>Symbol</th><th>Side</th><th>Type</th><th>Qty</th><th>Price</th><th>Status</th></tr>
            </thead>
            <tbody>
              <tr>
                <td style={{ fontFamily: 'monospace', fontSize: '12px' }}>ORD-001</td>
                <td>AAPL</td>
                <td style={{ color: '#00d4aa' }}>BUY</td>
                <td>Market</td>
                <td>100</td>
                <td>$175.50</td>
                <td><span style={{ ...styles.badge, backgroundColor: '#00d4aa22', color: '#00d4aa' }}>Filled</span></td>
              </tr>
              <tr>
                <td style={{ fontFamily: 'monospace', fontSize: '12px' }}>ORD-002</td>
                <td>GOOGL</td>
                <td style={{ color: '#ff6b6b' }}>SELL</td>
                <td>Limit</td>
                <td>50</td>
                <td>$142.00</td>
                <td><span style={{ ...styles.badge, backgroundColor: '#ffd43b22', color: '#ffd43b' }}>Pending</span></td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}

// ─── Generic List Pages ──────────────────────────────────────────────────────
function Traders() {
  return (
    <div>
      <h1 style={styles.pageTitle}>Trader Management</h1>
      <div style={styles.card}>
        <table style={styles.table}>
          <thead><tr><th>Trader ID</th><th>Username</th><th>Email</th><th>Level</th><th>KYC</th><th>Status</th></tr></thead>
          <tbody>
            <tr><td>TRD-001</td><td>trader_john</td><td>john@tradehub.com</td><td>Advanced</td><td style={{color:'#00d4aa'}}>Approved</td><td>Active</td></tr>
            <tr><td>TRD-002</td><td>trader_sarah</td><td>sarah@tradehub.com</td><td>Intermediate</td><td style={{color:'#00d4aa'}}>Approved</td><td>Active</td></tr>
            <tr><td>TRD-003</td><td>trader_mike</td><td>mike@tradehub.com</td><td>Beginner</td><td style={{color:'#ffd43b'}}>Pending</td><td>Active</td></tr>
          </tbody>
        </table>
      </div>
    </div>
  );
}

function Accounts() {
  return (
    <div>
      <h1 style={styles.pageTitle}>Trading Accounts</h1>
      <div style={styles.cardGrid}>
        <div style={{ ...styles.card, borderTop: '3px solid #00d4aa' }}><h3>Cash Accounts</h3><p style={{ fontSize: '28px', fontWeight: 'bold' }}>2</p></div>
        <div style={{ ...styles.card, borderTop: '3px solid #4dabf7' }}><h3>Margin Accounts</h3><p style={{ fontSize: '28px', fontWeight: 'bold' }}>1</p></div>
        <div style={{ ...styles.card, borderTop: '3px solid #ffd43b' }}><h3>Total Balance</h3><p style={{ fontSize: '28px', fontWeight: 'bold' }}>$238,216.93</p></div>
      </div>
    </div>
  );
}

function TradeHistory() {
  return (
    <div>
      <h1 style={styles.pageTitle}>Trade History</h1>
      <div style={styles.card}>
        <table style={styles.table}>
          <thead><tr><th>Trade ID</th><th>Symbol</th><th>Side</th><th>Qty</th><th>Price</th><th>Commission</th><th>Net Amount</th><th>Time</th></tr></thead>
          <tbody>
            <tr><td>TRD-EX-001</td><td>AAPL</td><td style={{color:'#00d4aa'}}>BUY</td><td>100</td><td>$175.50</td><td>$4.95</td><td>$17,554.95</td><td>2026-03-04 10:30:00</td></tr>
            <tr><td>TRD-EX-002</td><td>MSFT</td><td style={{color:'#ff6b6b'}}>SELL</td><td>50</td><td>$378.85</td><td>$4.95</td><td>$18,937.55</td><td>2026-03-04 11:15:00</td></tr>
          </tbody>
        </table>
      </div>
    </div>
  );
}

function Settlements() {
  return (
    <div>
      <h1 style={styles.pageTitle}>Settlements</h1>
      <div style={styles.card}>
        <table style={styles.table}>
          <thead><tr><th>Settlement ID</th><th>Trade ID</th><th>Amount</th><th>Settlement Date</th><th>Status</th></tr></thead>
          <tbody>
            <tr><td>STL-001</td><td>TRD-EX-001</td><td>$17,554.95</td><td>2026-03-06</td><td><span style={{...styles.badge, backgroundColor: '#ffd43b22', color: '#ffd43b'}}>Pending</span></td></tr>
            <tr><td>STL-002</td><td>TRD-EX-002</td><td>$18,937.55</td><td>2026-03-06</td><td><span style={{...styles.badge, backgroundColor: '#00d4aa22', color: '#00d4aa'}}>Settled</span></td></tr>
          </tbody>
        </table>
      </div>
    </div>
  );
}

// ─── App Component ───────────────────────────────────────────────────────────
function App() {
  return (
    <Router>
      <div style={styles.appContainer}>
        <Sidebar />
        <main style={styles.mainContent}>
          <Routes>
            <Route path="/" element={<Dashboard />} />
            <Route path="/portfolio" element={<Portfolio />} />
            <Route path="/orders" element={<Orders />} />
            <Route path="/market" element={<MarketData />} />
            <Route path="/traders" element={<Traders />} />
            <Route path="/accounts" element={<Accounts />} />
            <Route path="/trades" element={<TradeHistory />} />
            <Route path="/settlements" element={<Settlements />} />
          </Routes>
        </main>
      </div>
    </Router>
  );
}

// ─── Styles ──────────────────────────────────────────────────────────────────
const styles = {
  appContainer: { display: 'flex', minHeight: '100vh', backgroundColor: '#0a0a1a', color: '#e0e0e0', fontFamily: '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif' },
  sidebar: { width: '240px', backgroundColor: '#111827', borderRight: '1px solid #1e293b', padding: '0', position: 'fixed', height: '100vh', overflowY: 'auto' },
  logo: { padding: '20px 16px', borderBottom: '1px solid #1e293b' },
  navItem: { display: 'block', padding: '12px 16px', textDecoration: 'none', color: '#94a3b8', fontSize: '14px', transition: 'all 0.2s' },
  mainContent: { marginLeft: '240px', flex: 1, padding: '24px', minHeight: '100vh' },
  pageTitle: { fontSize: '24px', fontWeight: '600', marginBottom: '24px', color: '#f1f5f9' },
  cardGrid: { display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(250px, 1fr))', gap: '20px', marginBottom: '20px' },
  card: { backgroundColor: '#1e293b', borderRadius: '8px', padding: '20px', marginBottom: '16px' },
  table: { width: '100%', borderCollapse: 'collapse', fontSize: '14px' },
  badge: { display: 'inline-block', padding: '2px 8px', borderRadius: '4px', fontSize: '12px', backgroundColor: '#374151', color: '#94a3b8' },
  btn: { padding: '8px 16px', border: 'none', borderRadius: '6px', cursor: 'pointer', color: '#fff', fontSize: '14px' },
  input: { width: '100%', padding: '8px 12px', borderRadius: '6px', border: '1px solid #374151', backgroundColor: '#0f172a', color: '#e2e8f0', fontSize: '14px', boxSizing: 'border-box' },
  label: { display: 'block', marginBottom: '4px', fontSize: '12px', color: '#94a3b8', textTransform: 'uppercase' },
  loading: { display: 'flex', justifyContent: 'center', alignItems: 'center', height: '200px', color: '#888' },
};

// Global CSS overrides
const styleSheet = document.createElement('style');
styleSheet.textContent = `
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body { background-color: #0a0a1a; }
  table th { text-align: left; padding: 12px 8px; border-bottom: 2px solid #374151; color: #94a3b8; font-size: 12px; text-transform: uppercase; }
  table td { padding: 10px 8px; border-bottom: 1px solid #1e293b; }
  table tr:hover { background-color: #1e293b44; }
  a:hover { color: #00d4aa !important; background-color: #1e3a5f44 !important; }
  input:focus, select:focus { outline: none; border-color: #00d4aa; }
  button:hover { opacity: 0.9; }
  ::-webkit-scrollbar { width: 6px; }
  ::-webkit-scrollbar-track { background: #0a0a1a; }
  ::-webkit-scrollbar-thumb { background: #374151; border-radius: 3px; }
`;
document.head.appendChild(styleSheet);

export default App;
