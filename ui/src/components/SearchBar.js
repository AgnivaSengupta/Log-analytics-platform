import React, { useState } from 'react';

function SearchBar({ onSearch, query }) {
  const [service, setService] = useState(query.service || '');
  const [severity, setSeverity] = useState(query.severity || '');
  const [search, setSearch] = useState(query.search || '');
  const [timeRange, setTimeRange] = useState('24h');

  const handleSubmit = (e) => {
    e.preventDefault();
    const now = new Date();
    let startTime = new Date(now.getTime() - 24 * 60 * 60 * 1000);

    switch (timeRange) {
      case '1h': startTime = new Date(now.getTime() - 60 * 60 * 1000); break;
      case '6h': startTime = new Date(now.getTime() - 6 * 60 * 60 * 1000); break;
      case '24h': startTime = new Date(now.getTime() - 24 * 60 * 60 * 1000); break;
      case '7d': startTime = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000); break;
      case '30d': startTime = new Date(now.getTime() - 30 * 24 * 60 * 60 * 1000); break;
      default: break;
    }

    onSearch({
      service,
      severity,
      search,
      startTime: startTime.toISOString(),
      endTime: now.toISOString()
    });
  };

  return (
    <form onSubmit={handleSubmit} style={styles.form}>
      <div style={styles.row}>
        <div style={styles.field}>
          <label style={styles.label}>Service</label>
          <input
            type="text"
            value={service}
            onChange={(e) => setService(e.target.value)}
            placeholder="e.g., payment"
            style={styles.input}
          />
        </div>
        <div style={styles.field}>
          <label style={styles.label}>Severity</label>
          <select
            value={severity}
            onChange={(e) => setSeverity(e.target.value)}
            style={styles.select}
          >
            <option value="">All</option>
            <option value="DEBUG">Debug</option>
            <option value="INFO">Info</option>
            <option value="WARNING">Warning</option>
            <option value="ERROR">Error</option>
            <option value="FATAL">Fatal</option>
          </select>
        </div>
        <div style={styles.field}>
          <label style={styles.label}>Time Range</label>
          <select
            value={timeRange}
            onChange={(e) => setTimeRange(e.target.value)}
            style={styles.select}
          >
            <option value="1h">Last 1 hour</option>
            <option value="6h">Last 6 hours</option>
            <option value="24h">Last 24 hours</option>
            <option value="7d">Last 7 days</option>
            <option value="30d">Last 30 days</option>
          </select>
        </div>
        <div style={{...styles.field, flex: 2}}>
          <label style={styles.label}>Search</label>
          <input
            type="text"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            placeholder="Search in messages..."
            style={styles.input}
          />
        </div>
        <div style={styles.field}>
          <label style={styles.label}>&nbsp;</label>
          <button type="submit" style={styles.button}>
            🔍 Search
          </button>
        </div>
      </div>
    </form>
  );
}

const styles = {
  form: {
    backgroundColor: '#1e293b',
    padding: '1.5rem',
    borderRadius: '12px',
    border: '1px solid #334155'
  },
  row: {
    display: 'flex',
    gap: '1rem',
    alignItems: 'flex-end',
    flexWrap: 'wrap'
  },
  field: {
    display: 'flex',
    flexDirection: 'column',
    gap: '0.25rem',
    flex: 1,
    minWidth: '120px'
  },
  label: {
    fontSize: '0.75rem',
    color: '#94a3b8',
    fontWeight: 600,
    textTransform: 'uppercase',
    letterSpacing: '0.05em'
  },
  input: {
    padding: '0.6rem 0.8rem',
    backgroundColor: '#0f172a',
    border: '1px solid #475569',
    borderRadius: '6px',
    color: '#e2e8f0',
    fontSize: '0.9rem',
    outline: 'none'
  },
  select: {
    padding: '0.6rem 0.8rem',
    backgroundColor: '#0f172a',
    border: '1px solid #475569',
    borderRadius: '6px',
    color: '#e2e8f0',
    fontSize: '0.9rem',
    outline: 'none'
  },
  button: {
    padding: '0.6rem 1.5rem',
    backgroundColor: '#3b82f6',
    color: 'white',
    border: 'none',
    borderRadius: '6px',
    fontSize: '0.9rem',
    fontWeight: 600,
    cursor: 'pointer',
    whiteSpace: 'nowrap'
  }
};

export default SearchBar;
