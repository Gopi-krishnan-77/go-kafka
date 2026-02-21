import { useState, useEffect, useCallback, useRef } from 'react';
import { api } from './services/api';
import type {
  WeekendEvent,
  ListResult,
  AggregatedStats,
  Metrics,
  BrokerStats,
  PipelineInfo,
  CreateEventRequest,
} from './types';
import './index.css';

type Page = 'home' | 'dashboard' | 'broker';

// ─── Toast ────────────────────────────────────────

interface ToastData {
  message: string;
  type: 'success' | 'error';
}

function Toast({ toast, onClose }: { toast: ToastData | null; onClose: () => void }) {
  useEffect(() => {
    if (toast) {
      const t = setTimeout(onClose, 3000);
      return () => clearTimeout(t);
    }
  }, [toast, onClose]);
  if (!toast) return null;
  return (
    <div className={`toast toast-${toast.type}`}>
      {toast.type === 'success' ? '✓' : '✗'} {toast.message}
    </div>
  );
}

// ─── Create Event Modal ───────────────────────────

const CATEGORIES = [
  'outdoors', 'social', 'music', 'creative', 'food',
  'cooking', 'games', 'adventure', 'rest', 'sport', 'other',
];

function CreateEventModal({
  onClose,
  onCreated,
}: {
  onClose: () => void;
  onCreated: (msg: string) => void;
}) {
  const [name, setName] = useState('');
  const [category, setCategory] = useState('outdoors');
  const [moodBoost, setMoodBoost] = useState(5);
  const [tags, setTags] = useState('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError('');
    setLoading(true);
    try {
      const data: CreateEventRequest = {
        name: name.trim(),
        category,
        mood_boost: moodBoost,
      };
      const tagList = tags.split(',').map(t => t.trim()).filter(Boolean);
      if (tagList.length > 0) data.tags = tagList;
      await api.createEvent(data);
      onCreated(`"${name}" queued for processing!`);
      onClose();
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : 'Failed to create');
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="modal" onClick={e => e.stopPropagation()}>
        <h2>🎉 New Weekend Event</h2>
        <form onSubmit={handleSubmit}>
          <div className="form-group">
            <label>Activity Name</label>
            <input
              className="form-input"
              placeholder="e.g. Sunrise Hike, Board Games Night..."
              value={name}
              onChange={e => setName(e.target.value)}
              autoFocus
              required
            />
          </div>
          <div className="form-group">
            <label>Category</label>
            <select className="form-input" value={category} onChange={e => setCategory(e.target.value)}>
              {CATEGORIES.map(c => (
                <option key={c} value={c}>{c.charAt(0).toUpperCase() + c.slice(1)}</option>
              ))}
            </select>
          </div>
          <div className="form-group">
            <label>Mood Boost</label>
            <div className="slider-wrap">
              <input
                type="range"
                min={1}
                max={10}
                value={moodBoost}
                onChange={e => setMoodBoost(Number(e.target.value))}
              />
              <span className="slider-value">{moodBoost}</span>
            </div>
          </div>
          <div className="form-group">
            <label>Tags (comma-separated, optional)</label>
            <input
              className="form-input"
              placeholder="e.g. nature, exercise, friends"
              value={tags}
              onChange={e => setTags(e.target.value)}
            />
          </div>
          {error && <p style={{ color: 'var(--red)', fontSize: '0.85rem', marginBottom: 12 }}>{error}</p>}
          <div className="form-actions">
            <button type="button" className="btn btn-secondary" onClick={onClose}>Cancel</button>
            <button type="submit" className="btn btn-primary" disabled={loading}>
              {loading ? 'Creating...' : 'Create Event'}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}

// ─── Priority Badge ───────────────────────────────

function PriorityBadge({ priority }: { priority: string }) {
  return <span className={`priority-badge priority-${priority}`}>{priority}</span>;
}

// ─── Score Bar ────────────────────────────────────

function ScoreBar({ score }: { score: number }) {
  return (
    <div className="score-bar-wrap">
      <div className="score-bar">
        <div className="score-bar-fill" style={{ width: `${Math.min(100, score)}%` }} />
      </div>
      <span className="score-val">{score.toFixed(0)}</span>
    </div>
  );
}

// ─── Category Color ───────────────────────────────

function CategoryLabel({ category }: { category: string }) {
  return <span className={`cat-${category}`}>{category}</span>;
}

// ─── Hero Section ─────────────────────────────────

function HeroSection({ onNavigate }: { onNavigate: (p: Page) => void }) {
  return (
    <section className="hero" id="hero">
      <div className="hero-bg">
        <div className="hero-orb hero-orb-1" />
        <div className="hero-orb hero-orb-2" />
        <div className="hero-orb hero-orb-3" />
      </div>
      <div className="hero-content">
        <div className="hero-badge animate-in">
          <span className="dot" />
          Zero dependencies · Pure Go backend
        </div>
        <h1 className="hero-title animate-in stagger-1">
          <span className="gradient-text">Weekend Engine</span>
        </h1>
        <p className="hero-subtitle animate-in stagger-2">
          A sophisticated event-driven processing system with an in-memory message broker,
          multi-stage pipeline, indexed storage, and real-time analytics — all running without Kafka or Docker.
        </p>
        <div className="hero-actions animate-in stagger-3">
          <button className="btn btn-primary" onClick={() => onNavigate('dashboard')}>
            🚀 Open Dashboard
          </button>
          <button className="btn btn-secondary" onClick={() => {
            document.getElementById('features')?.scrollIntoView({ behavior: 'smooth' });
          }}>
            Learn More ↓
          </button>
        </div>
      </div>
    </section>
  );
}

// ─── Features Section ─────────────────────────────

const FEATURES = [
  {
    icon: '📡',
    title: 'In-Memory Broker',
    desc: 'Partitioned topics, consumer groups, offset tracking, dead-letter queues, and message replay — simulating Kafka entirely in memory.',
  },
  {
    icon: '⚡',
    title: '5-Stage Pipeline',
    desc: 'Events flow through validate → enrich → score → classify → persist stages with per-stage timeouts and error handling.',
  },
  {
    icon: '🗄️',
    title: 'Indexed Storage',
    desc: 'Thread-safe store with secondary indices, full-text search, pagination, and automatic TTL-based eviction.',
  },
  {
    icon: '🛡️',
    title: 'Middleware Stack',
    desc: 'Request ID, structured logging, token-bucket rate limiting, panic recovery, CORS, and security headers.',
  },
  {
    icon: '📊',
    title: 'Real-Time Analytics',
    desc: 'Counters, gauges, and latency histograms with p50/p95/p99 percentiles — all available via REST.',
  },
  {
    icon: '⏰',
    title: 'Background Scheduler',
    desc: 'Recurring jobs for TTL eviction and metrics snapshots with jitter to avoid thundering herd.',
  },
];

function FeaturesSection() {
  return (
    <section className="features" id="features">
      <div className="container">
        <div className="section-header">
          <span className="section-label">Architecture</span>
          <h2 className="section-title">Built for Complexity</h2>
          <p className="section-desc">
            Every component is production-grade, from the partitioned broker to the weighted scoring algorithm.
          </p>
        </div>
        <div className="features-grid">
          {FEATURES.map((f, i) => (
            <div key={i} className={`feature-card animate-in stagger-${i + 1}`}>
              <div className="feature-icon">{f.icon}</div>
              <h3>{f.title}</h3>
              <p>{f.desc}</p>
            </div>
          ))}
        </div>

        <div className="section-header" style={{ marginTop: 80 }}>
          <span className="section-label">How it works</span>
          <h2 className="section-title">Event Processing Flow</h2>
        </div>
        <div className="arch-flow">
          {[
            { emoji: '📝', title: 'HTTP API', desc: '11 REST endpoints' },
            { emoji: '📡', title: 'Broker', desc: 'Partitioned topics' },
            { emoji: '⚡', title: 'Pipeline', desc: '5-stage processing' },
            { emoji: '🗄️', title: 'Storage', desc: 'Indexed & searchable' },
            { emoji: '📊', title: 'Metrics', desc: 'Counters & latency' },
          ].map((n, i) => (
            <div key={i} className="arch-node">
              <div className="emoji">{n.emoji}</div>
              <h4>{n.title}</h4>
              <p>{n.desc}</p>
            </div>
          ))}
        </div>
      </div>
    </section>
  );
}

// ─── Dashboard ────────────────────────────────────

function Dashboard({ showToast }: { showToast: (msg: string, type: 'success' | 'error') => void }) {
  const [stats, setStats] = useState<AggregatedStats | null>(null);
  const [events, setEvents] = useState<ListResult | null>(null);
  const [metrics, setMetrics] = useState<Metrics | null>(null);
  const [pipelineInfo, setPipelineInfo] = useState<PipelineInfo | null>(null);
  const [loading, setLoading] = useState(true);
  const [showCreate, setShowCreate] = useState(false);
  const [searchQuery, setSearchQuery] = useState('');
  const [searchResults, setSearchResults] = useState<WeekendEvent[] | null>(null);
  const [page, setPage] = useState(1);
  const [catFilter, setCatFilter] = useState('');
  const [priFilter, setPriFilter] = useState('');
  const searchTimeout = useRef<ReturnType<typeof setTimeout>>(undefined);

  const fetchAll = useCallback(async () => {
    try {
      const [s, e, m, p] = await Promise.all([
        api.getStats(),
        api.listEvents({ page, limit: 10, category: catFilter, priority: priFilter }),
        api.getMetrics(),
        api.getPipelineInfo(),
      ]);
      setStats(s);
      setEvents(e);
      setMetrics(m);
      setPipelineInfo(p);
    } catch {
      showToast('Failed to load data — is the API running?', 'error');
    } finally {
      setLoading(false);
    }
  }, [page, catFilter, priFilter, showToast]);

  useEffect(() => { fetchAll(); }, [fetchAll]);

  // Auto-refresh every 5s
  useEffect(() => {
    const interval = setInterval(fetchAll, 5000);
    return () => clearInterval(interval);
  }, [fetchAll]);

  const handleSearch = useCallback((q: string) => {
    setSearchQuery(q);
    if (searchTimeout.current) clearTimeout(searchTimeout.current);
    if (!q.trim()) {
      setSearchResults(null);
      return;
    }
    searchTimeout.current = setTimeout(async () => {
      try {
        const res = await api.searchEvents(q);
        setSearchResults(res.results);
      } catch {
        setSearchResults([]);
      }
    }, 300);
  }, []);

  const handleDelete = async (id: string) => {
    try {
      await api.deleteEvent(id);
      showToast('Event deleted', 'success');
      fetchAll();
    } catch {
      showToast('Failed to delete', 'error');
    }
  };

  if (loading) {
    return (
      <div className="dashboard">
        <div className="container">
          <div className="loading"><div className="spinner" /> Loading dashboard...</div>
        </div>
      </div>
    );
  }

  const displayEvents = searchResults !== null ? searchResults : events?.events ?? [];

  return (
    <div className="dashboard">
      <div className="container">
        {/* Header */}
        <div className="dash-header">
          <h2>📊 Dashboard</h2>
          <div style={{ display: 'flex', gap: 10, alignItems: 'center', flexWrap: 'wrap' }}>
            <div className="search-bar">
              <span className="search-icon">🔍</span>
              <input
                placeholder="Search events..."
                value={searchQuery}
                onChange={e => handleSearch(e.target.value)}
              />
            </div>
            <button className="btn btn-primary" onClick={() => setShowCreate(true)}>
              + New Event
            </button>
          </div>
        </div>

        {/* Stats Cards */}
        {stats && (
          <div className="stats-grid animate-in">
            <div className="stat-card">
              <div className="stat-label">Total Events</div>
              <div className="stat-value accent">{stats.total_events}</div>
            </div>
            <div className="stat-card">
              <div className="stat-label">Avg Fun Score</div>
              <div className="stat-value green">{stats.avg_fun_score.toFixed(1)}</div>
            </div>
            <div className="stat-card">
              <div className="stat-label">Top Category</div>
              <div className="stat-value yellow" style={{ fontSize: '1.4rem' }}>
                {stats.top_categories?.[0]?.category || '—'}
              </div>
              <div className="stat-sub">
                {stats.top_categories?.[0]?.count || 0} events
              </div>
            </div>
            <div className="stat-card">
              <div className="stat-label">By Priority</div>
              <div style={{ marginTop: 6 }}>
                {Object.entries(stats.by_priority).map(([k, v]) => (
                  <div key={k} style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 2 }}>
                    <PriorityBadge priority={k} />
                    <span style={{ fontFamily: 'var(--font-mono)', fontSize: '0.85rem' }}>{v}</span>
                  </div>
                ))}
              </div>
            </div>
          </div>
        )}

        {/* Pipeline Visualization */}
        {pipelineInfo && (
          <div className="card">
            <div className="card-header">
              <h3>⚡ Pipeline Stages</h3>
              <span style={{ fontSize: '0.8rem', color: 'var(--text-muted)' }}>
                timeout: {pipelineInfo.timeout}
              </span>
            </div>
            <div className="pipeline-stages">
              {pipelineInfo.stages.map((stage, i) => (
                <div key={i} className="pipeline-stage">
                  <div className="stage-node">{stage}</div>
                  {i < pipelineInfo.stages.length - 1 && <span className="stage-arrow">→</span>}
                </div>
              ))}
            </div>
          </div>
        )}

        {/* Events Table */}
        <div className="card">
          <div className="card-header">
            <h3>📋 Events {searchResults !== null ? `(search: "${searchQuery}")` : ''}</h3>
            {searchResults === null && (
              <div className="filters">
                <select className="filter-select" value={catFilter} onChange={e => { setCatFilter(e.target.value); setPage(1); }}>
                  <option value="">All Categories</option>
                  {CATEGORIES.map(c => <option key={c} value={c}>{c}</option>)}
                </select>
                <select className="filter-select" value={priFilter} onChange={e => { setPriFilter(e.target.value); setPage(1); }}>
                  <option value="">All Priorities</option>
                  {['critical', 'high', 'medium', 'low'].map(p => <option key={p} value={p}>{p}</option>)}
                </select>
              </div>
            )}
          </div>

          {displayEvents.length === 0 ? (
            <div className="empty-state">
              <div className="emoji">🎯</div>
              <p>No events yet. Create your first weekend activity!</p>
              <button className="btn btn-primary" onClick={() => setShowCreate(true)}>
                + Create Event
              </button>
            </div>
          ) : (
            <>
              <div className="events-table-wrap">
                <table className="events-table">
                  <thead>
                    <tr>
                      <th>Name</th>
                      <th>Category</th>
                      <th>Score</th>
                      <th>Priority</th>
                      <th>Mood</th>
                      <th>Tags</th>
                      <th>Day</th>
                      <th></th>
                    </tr>
                  </thead>
                  <tbody>
                    {displayEvents.map(evt => (
                      <tr key={evt.id}>
                        <td style={{ fontWeight: 600 }}>{evt.name}</td>
                        <td><CategoryLabel category={evt.category} /></td>
                        <td><ScoreBar score={evt.fun_score} /></td>
                        <td><PriorityBadge priority={evt.priority} /></td>
                        <td style={{ textAlign: 'center' }}>
                          <span style={{ fontFamily: 'var(--font-mono)' }}>{evt.mood_boost}</span>
                        </td>
                        <td>
                          {evt.tags?.map((t, i) => <span key={i} className="tag">{t}</span>)}
                        </td>
                        <td style={{ color: 'var(--text-muted)', fontSize: '0.82rem' }}>
                          {evt.day_of_week?.slice(0, 3)}
                        </td>
                        <td>
                          <button className="btn btn-danger btn-sm" onClick={() => handleDelete(evt.id)}>
                            ✕
                          </button>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>

              {searchResults === null && events && (
                <div className="pagination">
                  <button disabled={page <= 1} onClick={() => setPage(p => Math.max(1, p - 1))}>
                    ← Prev
                  </button>
                  <span>Page {events.page} · {events.total} total</span>
                  <button disabled={page * 10 >= events.total} onClick={() => setPage(p => p + 1)}>
                    Next →
                  </button>
                </div>
              )}
            </>
          )}
        </div>

        {/* Score Distribution */}
        {stats && stats.total_events > 0 && (
          <div className="two-col">
            <div className="card">
              <div className="card-header">
                <h3>📈 Score Distribution</h3>
              </div>
              {Object.entries(stats.score_distribution)
                .sort(([a], [b]) => a.localeCompare(b))
                .map(([range, count]) => (
                  <div key={range} style={{ display: 'flex', alignItems: 'center', gap: 12, marginBottom: 10 }}>
                    <span style={{ fontFamily: 'var(--font-mono)', fontSize: '0.82rem', minWidth: 50, color: 'var(--text-secondary)' }}>
                      {range}
                    </span>
                    <div style={{ flex: 1, height: 8, background: 'rgba(255,255,255,0.05)', borderRadius: 4, overflow: 'hidden' }}>
                      <div style={{
                        height: '100%',
                        width: `${(count / stats.total_events) * 100}%`,
                        background: 'var(--gradient-score)',
                        borderRadius: 4,
                        transition: 'width 0.5s ease',
                      }} />
                    </div>
                    <span style={{ fontFamily: 'var(--font-mono)', fontSize: '0.85rem', minWidth: 24, textAlign: 'right' }}>
                      {count}
                    </span>
                  </div>
                ))}
            </div>

            <div className="card">
              <div className="card-header">
                <h3>📂 By Category</h3>
              </div>
              {Object.entries(stats.by_category)
                .sort(([, a], [, b]) => b - a)
                .map(([cat, count]) => (
                  <div key={cat} style={{ display: 'flex', alignItems: 'center', gap: 12, marginBottom: 10 }}>
                    <span className={`cat-${cat}`} style={{ fontWeight: 600, fontSize: '0.9rem', minWidth: 80 }}>
                      {cat}
                    </span>
                    <div style={{ flex: 1, height: 8, background: 'rgba(255,255,255,0.05)', borderRadius: 4, overflow: 'hidden' }}>
                      <div style={{
                        height: '100%',
                        width: `${(count / stats.total_events) * 100}%`,
                        background: 'var(--accent)',
                        borderRadius: 4,
                        transition: 'width 0.5s ease',
                      }} />
                    </div>
                    <span style={{ fontFamily: 'var(--font-mono)', fontSize: '0.85rem', minWidth: 24, textAlign: 'right' }}>
                      {count}
                    </span>
                  </div>
                ))}
            </div>
          </div>
        )}

        {/* Metrics */}
        {metrics && (
          <div className="card">
            <div className="card-header">
              <h3>📊 Live Metrics</h3>
              <span style={{ fontSize: '0.75rem', color: 'var(--text-muted)' }}>auto-refresh 5s</span>
            </div>
            <div className="metrics-grid">
              {Object.entries(metrics)
                .sort(([a], [b]) => a.localeCompare(b))
                .map(([key, val]) => (
                  <div key={key} className="metric-item">
                    <span className="metric-name">{key}</span>
                    <span className="metric-val">{typeof val === 'number' ? val.toLocaleString() : val}</span>
                  </div>
                ))}
            </div>
          </div>
        )}
      </div>

      {showCreate && (
        <CreateEventModal
          onClose={() => setShowCreate(false)}
          onCreated={msg => {
            showToast(msg, 'success');
            setTimeout(fetchAll, 500);
          }}
        />
      )}
    </div>
  );
}

// ─── Broker Page ──────────────────────────────────

function BrokerPage({ showToast }: { showToast: (msg: string, type: 'success' | 'error') => void }) {
  const [brokerStats, setBrokerStats] = useState<BrokerStats | null>(null);
  const [dlqMessages, setDlqMessages] = useState<unknown[]>([]);
  const [dlqTopic, setDlqTopic] = useState('');
  const [loading, setLoading] = useState(true);
  const [replayOffset, setReplayOffset] = useState('0');

  useEffect(() => {
    loadBroker();
  }, []);

  const loadBroker = async () => {
    try {
      const stats = await api.getBrokerTopics();
      setBrokerStats(stats);
    } catch {
      showToast('Failed to load broker stats', 'error');
    } finally {
      setLoading(false);
    }
  };

  const loadDLQ = async (topic: string) => {
    setDlqTopic(topic);
    try {
      const res = await api.getBrokerDLQ(topic);
      setDlqMessages(res.messages || []);
    } catch {
      showToast('Failed to load DLQ', 'error');
    }
  };

  const handleReplay = async (topic: string) => {
    try {
      const res = await api.replayMessages(topic, Number(replayOffset));
      showToast(`Replayed ${res.count} messages from offset ${replayOffset}`, 'success');
    } catch {
      showToast('Replay failed', 'error');
    }
  };

  if (loading) {
    return (
      <div className="dashboard">
        <div className="container">
          <div className="loading"><div className="spinner" /> Loading broker...</div>
        </div>
      </div>
    );
  }

  return (
    <div className="dashboard">
      <div className="container">
        <div className="dash-header">
          <h2>📡 Broker Introspection</h2>
        </div>

        {brokerStats && Object.entries(brokerStats.topics).map(([topic, ts]) => (
          <div key={topic} className="card">
            <div className="card-header">
              <h3>📌 Topic: <code style={{ color: 'var(--accent-light)' }}>{topic}</code></h3>
            </div>
            <div className="stats-grid" style={{ marginBottom: 20 }}>
              <div className="stat-card">
                <div className="stat-label">Published</div>
                <div className="stat-value accent">{ts.published}</div>
              </div>
              <div className="stat-card">
                <div className="stat-label">Partitions</div>
                <div className="stat-value green">{ts.partitions}</div>
              </div>
              <div className="stat-card">
                <div className="stat-label">DLQ Size</div>
                <div className="stat-value" style={{ color: ts.dlq_size > 0 ? 'var(--red)' : 'var(--text-muted)' }}>
                  {ts.dlq_size}
                </div>
              </div>
            </div>

            <div style={{ display: 'flex', gap: 10, flexWrap: 'wrap' }}>
              <button className="btn btn-secondary btn-sm" onClick={() => loadDLQ(topic)}>
                View DLQ
              </button>
              <div style={{ display: 'flex', gap: 6, alignItems: 'center' }}>
                <input
                  className="form-input"
                  style={{ width: 80, padding: '6px 10px', fontSize: '0.85rem' }}
                  placeholder="Offset"
                  type="number"
                  value={replayOffset}
                  onChange={e => setReplayOffset(e.target.value)}
                />
                <button className="btn btn-secondary btn-sm" onClick={() => handleReplay(topic)}>
                  Replay ↻
                </button>
              </div>
            </div>
          </div>
        ))}

        {!brokerStats || Object.keys(brokerStats.topics).length === 0 ? (
          <div className="card">
            <div className="empty-state">
              <div className="emoji">📡</div>
              <p>No topics yet. Create an event to initialize the broker.</p>
            </div>
          </div>
        ) : null}

        {dlqTopic && (
          <div className="card">
            <div className="card-header">
              <h3>💀 Dead Letter Queue — {dlqTopic}</h3>
            </div>
            {dlqMessages.length === 0 ? (
              <div className="empty-state" style={{ padding: 30 }}>
                <p>DLQ is empty — all messages processed successfully! 🎉</p>
              </div>
            ) : (
              <div style={{ maxHeight: 300, overflow: 'auto' }}>
                <pre style={{
                  fontSize: '0.8rem',
                  fontFamily: 'var(--font-mono)',
                  color: 'var(--text-secondary)',
                  whiteSpace: 'pre-wrap',
                }}>
                  {JSON.stringify(dlqMessages, null, 2)}
                </pre>
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
}

// ─── App ──────────────────────────────────────────

export default function App() {
  const [page, setPage] = useState<Page>('home');
  const [toast, setToast] = useState<ToastData | null>(null);
  const [mobileMenu, setMobileMenu] = useState(false);

  const showToast = useCallback((message: string, type: 'success' | 'error') => {
    setToast({ message, type });
  }, []);

  const navigate = (p: Page) => {
    setPage(p);
    setMobileMenu(false);
    window.scrollTo({ top: 0, behavior: 'smooth' });
  };

  return (
    <>
      {/* Navbar */}
      <nav className="navbar">
        <div className="container navbar-inner">
          <div className="navbar-logo" style={{ cursor: 'pointer' }} onClick={() => navigate('home')}>
            <span className="emoji">🎉</span>
            <span>Weekend Engine</span>
          </div>
          <div className="navbar-links">
            <button className={page === 'home' ? 'active' : ''} onClick={() => navigate('home')}>Home</button>
            <button className={page === 'dashboard' ? 'active' : ''} onClick={() => navigate('dashboard')}>Dashboard</button>
            <button className={page === 'broker' ? 'active' : ''} onClick={() => navigate('broker')}>Broker</button>
          </div>
          <div className="navbar-mobile-menu" style={{ display: 'none' }}>
            <button className="mobile-menu-btn" onClick={() => setMobileMenu(!mobileMenu)}>
              {mobileMenu ? '✕' : '☰'}
            </button>
          </div>
        </div>
      </nav>

      {/* Mobile menu */}
      {mobileMenu && (
        <div className="mobile-nav">
          <button className={page === 'home' ? 'active' : ''} onClick={() => navigate('home')}>🏠 Home</button>
          <button className={page === 'dashboard' ? 'active' : ''} onClick={() => navigate('dashboard')}>📊 Dashboard</button>
          <button className={page === 'broker' ? 'active' : ''} onClick={() => navigate('broker')}>📡 Broker</button>
        </div>
      )}

      {/* Pages */}
      {page === 'home' && (
        <>
          <HeroSection onNavigate={navigate} />
          <FeaturesSection />
          <footer className="footer">
            <div className="container">
              Weekend Engine · Built with Go & React · No Kafka Required
            </div>
          </footer>
        </>
      )}

      {page === 'dashboard' && <Dashboard showToast={showToast} />}
      {page === 'broker' && <BrokerPage showToast={showToast} />}

      {/* Toast */}
      <Toast toast={toast} onClose={() => setToast(null)} />
    </>
  );
}
