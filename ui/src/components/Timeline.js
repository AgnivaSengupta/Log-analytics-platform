import React from 'react';

function Timeline({ data }) {
  if (!data || data.length === 0) {
    return (
      <div style={styles.container}>
        <h3 style={styles.title}>📈 Event Timeline</h3>
        <div style={styles.empty}>No timeline data available</div>
      </div>
    );
  }

  const maxCount = Math.max(...data.map(d => d.count || 0));
  const maxErrors = Math.max(...data.map(d => d.error_count || 0));
  const chartHeight = 200;
  const barWidth = Math.max(4, Math.floor(800 / data.length) - 2);

  return (
    <div style={styles.container}>
      <h3 style={styles.title}>📈 Event Timeline</h3>
      <div style={styles.chart}>
        <svg width="100%" height={chartHeight + 40} viewBox={`0 0 ${data.length * (barWidth + 2) + 60} ${chartHeight + 40}`}>
          {/* Y-axis labels */}
          <text x="5" y="15" fill="#94a3b8" fontSize="10">{maxCount}</text>
          <text x="5" y={chartHeight} fill="#94a3b8" fontSize="10">0</text>

          {/* Bars */}
          {data.map((d, i) => {
            const height = maxCount > 0 ? (d.count / maxCount) * chartHeight : 0;
            const errorHeight = maxCount > 0 ? ((d.error_count || 0) / maxCount) * chartHeight : 0;
            const x = 50 + i * (barWidth + 2);

            return (
              <g key={i}>
                {/* Total events bar */}
                <rect
                  x={x}
                  y={chartHeight - height}
                  width={barWidth}
                  height={height}
                  fill="#3b82f6"
                  opacity="0.6"
                  rx="2"
                />
                {/* Error overlay */}
                <rect
                  x={x}
                  y={chartHeight - errorHeight}
                  width={barWidth}
                  height={errorHeight}
                  fill="#ef4444"
                  opacity="0.8"
                  rx="2"
                />
              </g>
            );
          })}

          {/* Baseline */}
          <line x1="50" y1={chartHeight} x2={50 + data.length * (barWidth + 2)} y2={chartHeight} stroke="#475569" strokeWidth="1" />
        </svg>
      </div>
      <div style={styles.legend}>
        <span style={styles.legendItem}>
          <span style={{...styles.legendDot, backgroundColor: '#3b82f6'}}></span>
          Total Events
        </span>
        <span style={styles.legendItem}>
          <span style={{...styles.legendDot, backgroundColor: '#ef4444'}}></span>
          Errors
        </span>
      </div>
    </div>
  );
}

const styles = {
  container: {
    backgroundColor: '#1e293b',
    padding: '1.5rem',
    borderRadius: '12px',
    border: '1px solid #334155'
  },
  title: {
    margin: '0 0 1rem 0',
    fontSize: '1.1rem',
    fontWeight: 600,
    color: '#f1f5f9'
  },
  chart: {
    overflowX: 'auto',
    padding: '0.5rem 0'
  },
  empty: {
    padding: '3rem',
    textAlign: 'center',
    color: '#64748b'
  },
  legend: {
    display: 'flex',
    gap: '1.5rem',
    marginTop: '0.5rem',
    justifyContent: 'center'
  },
  legendItem: {
    display: 'flex',
    alignItems: 'center',
    gap: '0.5rem',
    fontSize: '0.85rem',
    color: '#94a3b8'
  },
  legendDot: {
    width: '12px',
    height: '12px',
    borderRadius: '3px'
  }
};

export default Timeline;
