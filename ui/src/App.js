import React, { useState, useEffect, useCallback } from 'react';
import SearchBar from './components/SearchBar';
import Timeline from './components/Timeline';
import LogTable from './components/LogTable';
import MetricsPanel from './components/MetricsPanel';

const API_URL = process.env.REACT_APP_API_URL || 'http://localhost:8081';

function App() {
  const [logs, setLogs] = useState([]);
  const [total, setTotal] = useState(0);
  const [timeline, setTimeline] = useState([]);
  const [aggregates, setAggregates] = useState({});
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [query, setQuery] = useState({
    service: '',
    severity: '',
    search: '',
    startTime: new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString(),
    endTime: new Date().toISOString(),
    limit: 100,
    offset: 0
  });

  const fetchLogs = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const response = await fetch(`${API_URL}/v1/search`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(query)
      });
      if (!response.ok) throw new Error('Search failed');
      const data = await response.json();
      setLogs(data.results || []);
      setTotal(data.total || 0);
    } catch (err) {
      setError(err.message);
      console.error('Search error:', err);
    } finally {
      setLoading(false);
    }
  }, [query]);

  const fetchTimeline = useCallback(async () => {
    try {
      const params = new URLSearchParams({
        service: query.service,
        hours: '24'
      });
      const response = await fetch(`${API_URL}/v1/timeline?${params}`);
      if (!response.ok) throw new Error('Timeline fetch failed');
      const data = await response.json();
      setTimeline(data.timeline || []);
    } catch (err) {
      console.error('Timeline error:', err);
    }
  }, [query.service]);

  const fetchAggregates = useCallback(async () => {
    try {
      const params = new URLSearchParams({ hours: '24' });
      const response = await fetch(`${API_URL}/v1/aggregates?${params}`);
      if (!response.ok) throw new Error('Aggregates fetch failed');
      const data = await response.json();
      setAggregates(data.aggregates || {});
    } catch (err) {
      console.error('Aggregates error:', err);
    }
  }, []);

  useEffect(() => {
    fetchLogs();
    fetchTimeline();
    fetchAggregates();
  }, [fetchLogs, fetchTimeline, fetchAggregates]);

  const handleSearch = (newQuery) => {
    setQuery({ ...query, ...newQuery, offset: 0 });
  };

  const handlePageChange = (newOffset) => {
    setQuery({ ...query, offset: newOffset });
  };

  return (
    <div style={styles.container}>
      <header style={styles.header}>
        <h1 style={styles.title}>📊 Distributed Log Analytics</h1>
        <p style={styles.subtitle}>Real-time log search and analytics platform</p>
      </header>

      <div style={styles.content}>
        <SearchBar onSearch={handleSearch} query={query} />

        <div style={styles.grid}>
          <div style={styles.mainColumn}>
            {error && (
              <div style={styles.error}>
                ⚠️ {error}
              </div>
            )}

            <Timeline data={timeline} />

            <LogTable
              logs={logs}
              total={total}
              loading={loading}
              page={Math.floor(query.offset / query.limit)}
              pageSize={query.limit}
              onPageChange={handlePageChange}
            />
          </div>

          <div style={styles.sideColumn}>
            <MetricsPanel aggregates={aggregates} />
          </div>
        </div>
      </div>
    </div>
  );
}

const styles = {
  container: {
    fontFamily: '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif',
    backgroundColor: '#0f172a',
    color: '#e2e8f0',
    minHeight: '100vh',
    margin: 0,
    padding: 0
  },
  header: {
    background: 'linear-gradient(135deg, #1e293b 0%, #0f172a 100%)',
    padding: '2rem',
    borderBottom: '1px solid #334155'
  },
  title: {
    margin: 0,
    fontSize: '2rem',
    fontWeight: 700,
    color: '#f1f5f9'
  },
  subtitle: {
    margin: '0.5rem 0 0 0',
    color: '#94a3b8',
    fontSize: '1rem'
  },
  content: {
    padding: '2rem',
    maxWidth: '1600px',
    margin: '0 auto'
  },
  grid: {
    display: 'grid',
    gridTemplateColumns: '1fr 350px',
    gap: '2rem',
    marginTop: '2rem'
  },
  mainColumn: {
    display: 'flex',
    flexDirection: 'column',
    gap: '2rem'
  },
  sideColumn: {
    display: 'flex',
    flexDirection: 'column',
    gap: '1rem'
  },
  error: {
    padding: '1rem',
    backgroundColor: '#7f1d1d',
    border: '1px solid #dc2626',
    borderRadius: '8px',
    color: '#fecaca'
  }
};

export default App;
