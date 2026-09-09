import React, { useState } from 'react';

const severityColors = {
  DEBUG: '#64748b',
  INFO: '#3b82f6',
  WARNING: '#f59e0b',
  ERROR: '#ef4444',
  FATAL: '#dc2626'
};

function LogTable({ logs, total, loading, page, pageSize, onPageChange }) {
  const [expandedRow, setExpandedRow] = useState(null);
  const totalPages = Math.ceil(total / pageSize);

  const formatTimestamp = (ts) => {
    if (!ts) return '-';
    const d = new Date(ts);
    return d.toLocaleString();
  };

  return (
    <div style={styles.container}>
      <div style={styles.header}>
        <h3 style={styles.title}>📋 Log Events</h3>
        <span style={styles.count}>
          {total.toLocaleString()} events found
          {loading && <span style={styles.loading}> (loading...)</span>}
        </span>
      </div>

      {logs.length === 0 && !loading ? (
        <div style={styles.empty}>
          No events match your search criteria
        </div>
      ) : (
        <>
          <div style={styles.tableWrapper}>
            <table style={styles.table}>
              <thead>
                <tr>
                  <th style={styles.th}>Timestamp</th>
                  <th style={styles.th}>Severity</th>
                  <th style={styles.th}>Service</th>
                  <th style={styles.th}>Message</th>
                  <th style={styles.th}>Trace ID</th>
                </tr>
              </thead>
              <tbody>
                {logs.map((log, i) => (
                  <React.Fragment key={log.event_id || i}>
                    <tr
                      style={{
                        ...styles.tr,
                        ...(expandedRow === i ? styles.trExpanded : {})
                      }}
                      onClick={() => setExpandedRow(expandedRow === i ? null : i)}
                    >
                      <td style={styles.td}>
                        <span style={styles.timestamp}>
                          {formatTimestamp(log.timestamp)}
                        </span>
                      </td>
                      <td style={styles.td}>
                        <span style={{
                          ...styles.badge,
                          backgroundColor: severityColors[log.severity] || '#64748b'
                        }}>
                          {log.severity}
                        </span>
                      </td>
                      <td style={styles.td}>
                        <span style={styles.service}>{log.service}</span>
                      </td>
                      <td style={{...styles.td, maxWidth: '400px'}}>
                        <span style={styles.message}>
                          {log.message?.substring(0, 120)}
                          {log.message?.length > 120 ? '...' : ''}
                        </span>
                      </td>
                      <td style={styles.td}>
                        <span style={styles.traceId}>
                          {log.trace_id ? log.trace_id.substring(0, 12) + '...' : '-'}
                        </span>
                      </td>
                    </tr>
                    {expandedRow === i && (
                      <tr>
                        <td colSpan="5" style={styles.expandedContent}>
                          <div style={styles.detailSection}>
                            <h4 style={styles.detailTitle}>Event Details</h4>
                            <div style={styles.detailGrid}>
                              <div style={styles.detailItem}>
                                <span style={styles.detailLabel}>Event ID</span>
                                <span style={styles.detailValue}>{log.event_id}</span>
                              </div>
                              <div style={styles.detailItem}>
                                <span style={styles.detailLabel}>Source</span>
                                <span style={styles.detailValue}>{log.source || '-'}</span>
                              </div>
                              <div style={styles.detailItem}>
                                <span style={styles.detailLabel}>Region</span>
                                <span style={styles.detailValue}>{log.region || '-'}</span>
                              </div>
                              <div style={styles.detailItem}>
                                <span style={styles.detailLabel}>Version</span>
                                <span style={styles.detailValue}>{log.version || '-'}</span>
                              </div>
                            </div>
                            {log.attributes && (
                              <>
                                <h4 style={{...styles.detailTitle, marginTop: '1rem'}}>Attributes</h4>
                                <pre style={styles.jsonBlock}>
                                  {JSON.stringify(log.attributes, null, 2)}
                                </pre>
                              </>
                            )}
                            <h4 style={{...styles.detailTitle, marginTop: '1rem'}}>Full Message</h4>
                            <pre style={styles.jsonBlock}>{log.message}</pre>
                          </div>
                        </td>
                      </tr>
                    )}
                  </React.Fragment>
                ))}
              </tbody>
            </table>
          </div>

          {/* Pagination */}
          {totalPages > 1 && (
            <div style={styles.pagination}>
              <button
                style={styles.pageButton}
                disabled={page === 0}
                onClick={() => onPageChange((page - 1) * pageSize)}
              >
                ← Previous
              </button>
              <span style={styles.pageInfo}>
                Page {page + 1} of {totalPages}
              </span>
              <button
                style={styles.pageButton}
                disabled={page >= totalPages - 1}
                onClick={() => onPageChange((page + 1) * pageSize)}
              >
                Next →
              </button>
            </div>
          )}
        </>
      )}
    </div>
  );
}

const styles = {
  container: {
    backgroundColor: '#1e293b',
    borderRadius: '12px',
    border: '1px solid #334155',
    overflow: 'hidden'
  },
  header: {
    display: 'flex',
    justifyContent: 'space-between',
    alignItems: 'center',
    padding: '1.5rem 1.5rem 1rem 1.5rem'
  },
  title: {
    margin: 0,
    fontSize: '1.1rem',
    fontWeight: 600,
    color: '#f1f5f9'
  },
  count: {
    fontSize: '0.85rem',
    color: '#94a3b8'
  },
  loading: {
    color: '#f59e0b'
  },
  empty: {
    padding: '3rem',
    textAlign: 'center',
    color: '#64748b'
  },
  tableWrapper: {
    overflowX: 'auto'
  },
  table: {
    width: '100%',
    borderCollapse: 'collapse',
    fontSize: '0.85rem'
  },
  th: {
    padding: '0.75rem 1rem',
    textAlign: 'left',
    backgroundColor: '#0f172a',
    color: '#94a3b8',
    fontWeight: 600,
    fontSize: '0.75rem',
    textTransform: 'uppercase',
    letterSpacing: '0.05em',
    borderBottom: '1px solid #334155'
  },
  tr: {
    cursor: 'pointer',
    transition: 'background-color 0.15s',
    borderBottom: '1px solid #1e293b'
  },
  trExpanded: {
    backgroundColor: '#334155'
  },
  td: {
    padding: '0.75rem 1rem',
    borderBottom: '1px solid #334155'
  },
  timestamp: {
    fontFamily: 'monospace',
    fontSize: '0.8rem',
    color: '#94a3b8'
  },
  badge: {
    display: 'inline-block',
    padding: '2px 8px',
    borderRadius: '4px',
    fontSize: '0.7rem',
    fontWeight: 700,
    color: 'white',
    textTransform: 'uppercase'
  },
  service: {
    color: '#8b5cf6',
    fontWeight: 500
  },
  message: {
    color: '#cbd5e1',
    fontFamily: 'monospace',
    fontSize: '0.8rem',
    lineHeight: 1.4
  },
  traceId: {
    fontFamily: 'monospace',
    fontSize: '0.75rem',
    color: '#64748b'
  },
  expandedContent: {
    padding: 0,
    backgroundColor: '#0f172a'
  },
  detailSection: {
    padding: '1rem 1.5rem'
  },
  detailTitle: {
    margin: '0 0 0.5rem 0',
    fontSize: '0.85rem',
    fontWeight: 600,
    color: '#94a3b8'
  },
  detailGrid: {
    display: 'grid',
    gridTemplateColumns: 'repeat(auto-fill, minmax(250px, 1fr))',
    gap: '0.5rem'
  },
  detailItem: {
    display: 'flex',
    flexDirection: 'column',
    gap: '0.2rem'
  },
  detailLabel: {
    fontSize: '0.7rem',
    color: '#64748b',
    textTransform: 'uppercase'
  },
  detailValue: {
    fontSize: '0.85rem',
    color: '#e2e8f0',
    fontFamily: 'monospace'
  },
  jsonBlock: {
    backgroundColor: '#1e293b',
    padding: '0.75rem',
    borderRadius: '6px',
    fontSize: '0.8rem',
    color: '#a5b4fc',
    overflow: 'auto',
    maxHeight: '200px',
    margin: '0.5rem 0'
  },
  pagination: {
    display: 'flex',
    justifyContent: 'center',
    alignItems: 'center',
    gap: '1rem',
    padding: '1rem'
  },
  pageButton: {
    padding: '0.5rem 1rem',
    backgroundColor: '#334155',
    color: '#e2e8f0',
    border: 'none',
    borderRadius: '6px',
    cursor: 'pointer',
    fontSize: '0.85rem'
  },
  pageInfo: {
    color: '#94a3b8',
    fontSize: '0.85rem'
  }
};

export default LogTable;
