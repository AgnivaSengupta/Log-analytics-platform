import React from 'react';

function MetricsPanel({ aggregates }) {
  const services = Object.keys(aggregates || {});

  // Calculate totals
  let totalEvents = 0;
  let totalErrors = 0;
  const serviceStats = [];

  services.forEach(service => {
    const severities = aggregates[service];
    let serviceTotal = 0;
    let serviceErrors = 0;

    Object.entries(severities).forEach(([severity, count]) => {
      serviceTotal += count;
      if (severity === 'ERROR' || severity === 'FATAL' || severity === 'CRITICAL') {
        serviceErrors += count;
      }
    });

    totalEvents += serviceTotal;
    totalErrors += serviceErrors;

    serviceStats.push({
      service,
      total: serviceTotal,
      errors: serviceErrors,
      errorRate: serviceTotal > 0 ? (serviceErrors / serviceTotal * 100) : 0
    });
  });

  serviceStats.sort((a, b) => b.total - a.total);

  const overallErrorRate = totalEvents > 0 ? (totalErrors / totalEvents * 100) : 0;

  return (
    <div style={styles.container}>
      {/* Summary Cards */}
      <div style={styles.summaryGrid}>
        <div style={styles.card}>
          <div style={styles.cardValue}>{totalEvents.toLocaleString()}</div>
          <div style={styles.cardLabel}>Total Events</div>
        </div>
        <div style={styles.card}>
          <div style={{...styles.cardValue, color: totalErrors > 0 ? '#ef4444' : '#22c55e'}}>
            {totalErrors.toLocaleString()}
          </div>
          <div style={styles.cardLabel}>Errors</div>
        </div>
        <div style={styles.card}>
          <div style={{
            ...styles.cardValue,
            color: overallErrorRate > 5 ? '#ef4444' : overallErrorRate > 1 ? '#f59e0b' : '#22c55e'
          }}>
            {overallErrorRate.toFixed(2)}%
          </div>
          <div style={styles.cardLabel}>Error Rate</div>
        </div>
        <div style={styles.card}>
          <div style={styles.cardValue}>{services.length}</div>
          <div style={styles.cardLabel}>Services</div>
        </div>
      </div>

      {/* Service Breakdown */}
      <div style={styles.section}>
        <h3 style={styles.sectionTitle}>Service Breakdown</h3>
        {serviceStats.length === 0 ? (
          <div style={styles.empty}>No data available</div>
        ) : (
          <div style={styles.serviceList}>
            {serviceStats.map(stat => (
              <div key={stat.service} style={styles.serviceItem}>
                <div style={styles.serviceHeader}>
                  <span style={styles.serviceName}>{stat.service}</span>
                  <span style={styles.serviceCount}>
                    {stat.total.toLocaleString()}
                  </span>
                </div>
                <div style={styles.barContainer}>
                  <div style={{
                    ...styles.bar,
                    width: `${Math.min(100, (stat.total / (serviceStats[0]?.total || 1)) * 100)}%`,
                    backgroundColor: '#3b82f6'
                  }} />
                  {stat.errors > 0 && (
                    <div style={{
                      ...styles.barOverlay,
                      width: `${(stat.errors / stat.total) * 100}%`,
                      backgroundColor: '#ef4444'
                    }} />
                  )}
                </div>
                <div style={styles.serviceMeta}>
                  <span style={styles.errorRate}>
                    {stat.errors > 0 ? `${stat.errors} errors (${stat.errorRate.toFixed(1)}%)` : 'No errors'}
                  </span>
                </div>
              </div>
            ))}
          </div>
        )}
      </div>

      {/* Severity Distribution */}
      <div style={styles.section}>
        <h3 style={styles.sectionTitle}>Severity Distribution</h3>
        <SeverityBreakdown aggregates={aggregates} />
      </div>
    </div>
  );
}

function SeverityBreakdown({ aggregates }) {
  const totals = {};
  Object.values(aggregates || {}).forEach(severities => {
    Object.entries(severities).forEach(([severity, count]) => {
      totals[severity] = (totals[severity] || 0) + count;
    });
  });

  const grandTotal = Object.values(totals).reduce((a, b) => a + b, 0);
  const severityOrder = ['FATAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG'];
  const colors = {
    FATAL: '#dc2626',
    ERROR: '#ef4444',
    WARNING: '#f59e0b',
    INFO: '#3b82f6',
    DEBUG: '#64748b'
  };

  const sorted = severityOrder.filter(s => totals[s]).map(s => ({
    severity: s,
    count: totals[s],
    pct: grandTotal > 0 ? (totals[s] / grandTotal * 100) : 0
  }));

  return (
    <div>
      {sorted.length === 0 ? (
        <div style={styles.empty}>No data</div>
      ) : (
        sorted.map(item => (
          <div key={item.severity} style={styles.severityRow}>
            <div style={styles.severityHeader}>
              <span style={{...styles.severityDot, backgroundColor: colors[item.severity]}} />
              <span style={styles.severityLabel}>{item.severity}</span>
              <span style={styles.severityCount}>{item.count.toLocaleString()}</span>
              <span style={styles.severityPct}>{item.pct.toFixed(1)}%</span>
            </div>
            <div style={styles.barContainer}>
              <div style={{
                ...styles.bar,
                width: `${item.pct}%`,
                backgroundColor: colors[item.severity]
              }} />
            </div>
          </div>
        ))
      )}
    </div>
  );
}

const styles = {
  container: {
    display: 'flex',
    flexDirection: 'column',
    gap: '1rem'
  },
  summaryGrid: {
    display: 'grid',
    gridTemplateColumns: 'repeat(2, 1fr)',
    gap: '0.75rem'
  },
  card: {
    backgroundColor: '#1e293b',
    padding: '1rem',
    borderRadius: '10px',
    border: '1px solid #334155',
    textAlign: 'center'
  },
  cardValue: {
    fontSize: '1.5rem',
    fontWeight: 700,
    color: '#f1f5f9',
    fontFamily: 'monospace'
  },
  cardLabel: {
    fontSize: '0.7rem',
    color: '#94a3b8',
    textTransform: 'uppercase',
    letterSpacing: '0.05em',
    marginTop: '0.25rem'
  },
  section: {
    backgroundColor: '#1e293b',
    padding: '1.25rem',
    borderRadius: '10px',
    border: '1px solid #334155'
  },
  sectionTitle: {
    margin: '0 0 1rem 0',
    fontSize: '0.9rem',
    fontWeight: 600,
    color: '#f1f5f9'
  },
  empty: {
    padding: '1rem',
    textAlign: 'center',
    color: '#64748b',
    fontSize: '0.85rem'
  },
  serviceList: {
    display: 'flex',
    flexDirection: 'column',
    gap: '0.75rem'
  },
  serviceItem: {
    display: 'flex',
    flexDirection: 'column',
    gap: '0.25rem'
  },
  serviceHeader: {
    display: 'flex',
    justifyContent: 'space-between',
    alignItems: 'center'
  },
  serviceName: {
    fontSize: '0.85rem',
    fontWeight: 500,
    color: '#e2e8f0'
  },
  serviceCount: {
    fontSize: '0.8rem',
    color: '#94a3b8',
    fontFamily: 'monospace'
  },
  barContainer: {
    height: '6px',
    backgroundColor: '#0f172a',
    borderRadius: '3px',
    overflow: 'hidden',
    position: 'relative'
  },
  bar: {
    height: '100%',
    borderRadius: '3px',
    transition: 'width 0.5s ease'
  },
  barOverlay: {
    position: 'absolute',
    top: 0,
    left: 0,
    height: '100%',
    borderRadius: '3px',
    opacity: 0.8
  },
  serviceMeta: {
    display: 'flex',
    justifyContent: 'flex-end'
  },
  errorRate: {
    fontSize: '0.7rem',
    color: '#ef4444'
  },
  severityRow: {
    marginBottom: '0.75rem'
  },
  severityHeader: {
    display: 'flex',
    alignItems: 'center',
    gap: '0.5rem',
    marginBottom: '0.25rem'
  },
  severityDot: {
    width: '8px',
    height: '8px',
    borderRadius: '50%',
    flexShrink: 0
  },
  severityLabel: {
    fontSize: '0.8rem',
    fontWeight: 600,
    color: '#e2e8f0',
    flex: 1
  },
  severityCount: {
    fontSize: '0.8rem',
    color: '#94a3b8',
    fontFamily: 'monospace'
  },
  severityPct: {
    fontSize: '0.75rem',
    color: '#64748b',
    fontFamily: 'monospace',
    width: '45px',
    textAlign: 'right'
  }
};

export default MetricsPanel;
