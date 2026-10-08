import React, { useEffect, useState } from 'react';
import './App.css';

interface HealthResponse {
  status: string;
  dependencies: {
    postgres: 'connected' | 'disconnected';
    redis: 'connected' | 'disconnected';
  };
}

export const App: React.FC = () => {
  const [health, setHealth] = useState<HealthResponse | null>(null);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchHealth = async () => {
      try {
        const response = await fetch('/api/health');
        if (!response.ok) {
          throw new Error(`HTTP ${response.status}`);
        }
        const data: HealthResponse = await response.json();
        setHealth(data);
        setError(null);
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Failed to reach backend');
      } finally {
        setLoading(false);
      }
    };

    fetchHealth();
    const interval = setInterval(fetchHealth, 5000);
    return () => clearInterval(interval);
  }, []);

  return (
    <div className="app-container">
      <header className="header">
        <div className="brand">
          <div className="brand-icon">⚡</div>
          <span className="brand-title">RustWebhook</span>
        </div>
        <span className="badge-dev">Local Dev Ready</span>
      </header>

      <section className="hero">
        <h1>Local Development Environment</h1>
        <p>
          Frontend, backend, database, and cache services are initialized and communicating inside Docker.
        </p>
      </section>

      <div className="grid-status">
        <div className="card">
          <div className="card-header">
            <span className="service-name">Frontend (React + Vite)</span>
            <span className="status-pill healthy">
              <span className="pulse-dot"></span>
              Active
            </span>
          </div>
          <div className="card-meta">
            <div className="meta-row">
              <span>Stack:</span>
              <span>React 19 + TypeScript</span>
            </div>
            <div className="meta-row">
              <span>Port:</span>
              <span>5173 (HMR Active)</span>
            </div>
          </div>
        </div>

        <div className="card">
          <div className="card-header">
            <span className="service-name">Backend (Axum)</span>
            <span className={`status-pill ${loading ? 'degraded' : error ? 'disconnected' : 'healthy'}`}>
              <span className="pulse-dot"></span>
              {loading ? 'Checking...' : error ? 'Offline' : 'Connected'}
            </span>
          </div>
          <div className="card-meta">
            <div className="meta-row">
              <span>Stack:</span>
              <span>Rust 1.98.1 + Tokio</span>
            </div>
            <div className="meta-row">
              <span>Port:</span>
              <span>8000</span>
            </div>
          </div>
        </div>

        <div className="card">
          <div className="card-header">
            <span className="service-name">Database (PostgreSQL)</span>
            <span
              className={`status-pill ${
                health?.dependencies.postgres === 'connected' ? 'connected' : 'disconnected'
              }`}
            >
              <span className="pulse-dot"></span>
              {health?.dependencies.postgres || 'Unknown'}
            </span>
          </div>
          <div className="card-meta">
            <div className="meta-row">
              <span>Version:</span>
              <span>PostgreSQL 17.4</span>
            </div>
            <div className="meta-row">
              <span>Port:</span>
              <span>5432</span>
            </div>
          </div>
        </div>

        <div className="card">
          <div className="card-header">
            <span className="service-name">Cache / Queue (Redis)</span>
            <span
              className={`status-pill ${
                health?.dependencies.redis === 'connected' ? 'connected' : 'disconnected'
              }`}
            >
              <span className="pulse-dot"></span>
              {health?.dependencies.redis || 'Unknown'}
            </span>
          </div>
          <div className="card-meta">
            <div className="meta-row">
              <span>Version:</span>
              <span>Redis 7.4</span>
            </div>
            <div className="meta-row">
              <span>Port:</span>
              <span>6379</span>
            </div>
          </div>
        </div>
      </div>

      <div className="info-banner">
        <div className="info-banner-icon">ℹ️</div>
        <div className="info-banner-content">
          <h4>Clean Foundation Mode</h4>
          <p>
            No business domain features have been implemented yet. The local infrastructure is fully configured,
            hot-reloading, and ready for feature development.
          </p>
        </div>
      </div>
    </div>
  );
};

export default App;
