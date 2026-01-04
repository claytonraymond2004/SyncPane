import { useState, useEffect, useMemo } from 'react';
import { createPortal } from 'react-dom';
import { Activity, ArrowUp, Search, ChevronLeft, ChevronRight, Pause, Play, X, Trash2 } from 'lucide-react';
import ConnectionErrorCountdown from '../components/ConnectionErrorCountdown';
import ModalBackdrop from '../components/ModalBackdrop';
import PullToRefresh from '../components/PullToRefresh';

export default function Jobs() {
    const [jobs, setJobs] = useState([]);
    const [search, setSearch] = useState('');
    const [page, setPage] = useState(1);
    const [itemsPerPage, setItemsPerPage] = useState(25);
    const [totalPages, setTotalPages] = useState(1);
    const [totalItems, setTotalItems] = useState(0);
    const [jobStats, setJobStats] = useState({ active: 0, paused: 0 });

    const fetchJobs = async () => {
        try {
            const params = new URLSearchParams({
                page,
                limit: itemsPerPage,
                search
            });
            const res = await fetch(`/api/jobs?${params}`);
            const data = await res.json();

            if (data.pagination) {
                setJobs(data.data);
                setTotalPages(data.pagination.totalPages);
                setTotalItems(data.pagination.total);
                if (data.stats) setJobStats(data.stats);
            } else {
                // Fallback for older API response if needed, though we just changed it
                setJobs(data);
            }
        } catch (err) {
            console.error(err);
        }
    };

    const pauseAll = async () => {
        try {
            await fetch('/api/jobs/pause-all', { method: 'POST' });
            fetchJobs();
        } catch (err) {
            console.error('Failed to pause all:', err);
        }
    };

    const resumeAll = async () => {
        try {
            await fetch('/api/jobs/resume-all', { method: 'POST' });
            fetchJobs();
        } catch (err) {
            console.error('Failed to resume all:', err);
        }
    };

    useEffect(() => {
        fetchJobs();
        const interval = setInterval(fetchJobs, 2000); // Poll status
        return () => clearInterval(interval);
    }, [page, itemsPerPage, search]); // Re-fetch when params change

    const setPriority = async (id, priority) => {
        await fetch(`/api/jobs/${id}/priority`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ priority })
        });
        fetchJobs();
    }

    const updateJobStatus = async (id, action) => {
        // action: 'cancel', 'pause', 'resume'
        try {
            await fetch(`/api/jobs/${id}/${action}`, {
                method: 'POST'
            });
            fetchJobs();
        } catch (err) {
            console.error('Failed to update job status:', err);
        }
    };

    // Reset page on search/limit change
    useEffect(() => {
        setPage(1);
    }, [search, itemsPerPage]);

    // Helpers
    const formatBytes = (bytes) => {
        if (!bytes) return '0 B';
        const k = 1024;
        const i = Math.floor(Math.log(bytes) / Math.log(k));
        const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
        return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i];
    };

    const formatDuration = (seconds) => {
        if (!seconds) return '-';
        if (seconds < 60) return `${Math.round(seconds)}s`;
        const minutes = Math.floor(seconds / 60);
        return `${minutes}m ${Math.round(seconds % 60)}s`;
    };

    const getProgress = (job) => {
        if (!job.total_bytes || job.total_bytes === 0) return 0;
        return Math.min(100, (job.processed_bytes / job.total_bytes) * 100);
    };

    const [failedItems, setFailedItems] = useState(null); // { jobId, items }

    // Helpers
    const getFailedItems = (job) => {
        if (!job.failed_items) return [];
        try {
            return JSON.parse(job.failed_items);
        } catch (e) {
            return [];
        }
    };

    return (
        <div className="animate-enter">
            {/* Modal for Failed Items */}
            {failedItems && createPortal(
                <ModalBackdrop onClose={() => setFailedItems(null)}>
                    {/* ... (keep existing modal content) ... */}
                    <div style={{ padding: 24, paddingBottom: 16 }}>
                        <h2 style={{ fontSize: '1.2rem', marginBottom: 8 }}>Sync Failures</h2>
                        <p style={{ color: 'var(--text-muted)', marginBottom: 16, fontSize: '0.9em' }}>
                            Success: Partial. The following files could not be synced.
                        </p>

                        <div style={{
                            background: 'rgba(0,0,0,0.3)',
                            borderRadius: 6,
                            border: '1px solid rgba(255,255,255,0.1)',
                            maxHeight: '60vh',
                            overflowY: 'auto',
                            padding: 12
                        }}>
                            {failedItems.items.map((item, i) => (
                                <div key={i} style={{ marginBottom: 12, paddingBottom: 12, borderBottom: '1px solid rgba(255,255,255,0.05)' }}>
                                    <div style={{ fontFamily: 'monospace', fontSize: '0.9em', color: 'var(--error)' }}>
                                        {item.path}
                                    </div>
                                    <div style={{ fontSize: '0.8em', color: 'var(--text-muted)', marginTop: 2 }}>
                                        {item.error}
                                    </div>
                                </div>
                            ))}
                        </div>

                        <div style={{ display: 'flex', justifyContent: 'end', marginTop: 20 }}>
                            <button className="btn btn-secondary" onClick={() => setFailedItems(null)}>
                                Close
                            </button>
                        </div>
                    </div>
                </ModalBackdrop>,
                document.body
            )}

            <PullToRefresh onRefresh={fetchJobs}>
                {/* ... Existing header/controls ... */}
                <div className="header">
                    <h1 className="page-title">Job Monitor</h1>
                    <p style={{ color: 'var(--text-muted)' }}>Real-time sync activity and history.</p>
                </div>

                {/* Controls */}
                <div className="card" style={{ marginBottom: 16, padding: 12, display: 'flex', gap: 12, alignItems: 'center', flexWrap: 'wrap' }}>
                    <div style={{ position: 'relative', flex: 1, minWidth: 200 }}>
                        <Search size={16} style={{ position: 'absolute', left: 12, top: '50%', transform: 'translateY(-50%)', color: 'var(--text-muted)' }} />
                        <input
                            type="text"
                            placeholder="Search logs, status, paths..."
                            value={search}
                            onChange={e => setSearch(e.target.value)}
                            style={{
                                width: '100%', padding: '8px 12px 8px 36px',
                                borderRadius: 6, border: '1px solid rgba(255,255,255,0.1)',
                                background: 'rgba(0,0,0,0.2)', color: 'white'
                            }}
                        />
                    </div>

                    <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                        <span style={{ fontSize: '0.9em', color: 'var(--text-muted)' }}>Rows:</span>
                        <select
                            value={itemsPerPage}
                            onChange={e => setItemsPerPage(Number(e.target.value))}
                            style={{
                                padding: '6px 12px', borderRadius: 6,
                                background: 'rgba(0,0,0,0.2)', border: '1px solid rgba(255,255,255,0.1)', color: 'white'
                            }}
                        >
                            <option value={10}>10</option>
                            <option value={25}>25</option>
                            <option value={50}>50</option>
                            <option value={100}>100</option>
                        </select>
                    </div>

                    <div style={{ display: 'flex', gap: 8 }}>
                        <button
                            onClick={pauseAll}
                            disabled={jobStats.active === 0}
                            className="btn btn-secondary"
                            style={{
                                display: 'flex', gap: 6, alignItems: 'center',
                                opacity: jobStats.active === 0 ? 0.5 : 1,
                                cursor: jobStats.active === 0 ? 'not-allowed' : 'pointer'
                            }}
                        >
                            <Pause size={16} />
                            Pause All
                        </button>
                        <button
                            onClick={resumeAll}
                            disabled={jobStats.paused === 0}
                            className="btn btn-primary"
                            style={{
                                display: 'flex', gap: 6, alignItems: 'center',
                                opacity: jobStats.paused === 0 ? 0.5 : 1,
                                cursor: jobStats.paused === 0 ? 'not-allowed' : 'pointer'
                            }}
                        >
                            <Play size={16} />
                            Start All
                        </button>
                    </div>
                </div>

                <div className="card">
                    <div className="table-wrap">
                        <table className="fixed-table">
                            <thead>
                                <tr>
                                    <th style={{ width: 110 }}>Status</th>
                                    <th style={{ width: '30%' }}>Progress</th>
                                    <th style={{ width: 105 }}>Duration</th>
                                    <th style={{ width: '30%' }}>File/Folder</th>
                                    <th style={{ width: 160 }}>Started</th>
                                    <th style={{ textAlign: 'right', width: 140 }}>Actions</th>
                                </tr>
                            </thead>
                            <tbody>
                                {jobs.map(job => {
                                    const failedList = getFailedItems(job);
                                    const hasFailures = failedList.length > 0;

                                    return (
                                        <tr key={job.id}>
                                            <td data-label="Status">
                                                <span className={`status-badge status-${job.status}`}>
                                                    {job.status}
                                                </span>
                                            </td>
                                            <td data-label="Progress">
                                                {job.status === 'running' ? (
                                                    <div style={{ width: '100%' }}>
                                                        <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: '0.8em', marginBottom: 4 }}>
                                                            <span>{job.total_bytes > 0 ? `${formatBytes(job.processed_bytes)} / ${formatBytes(job.total_bytes)}` : 'Preparing...'}</span>
                                                            <span>{Math.round(getProgress(job))}%</span>
                                                        </div>
                                                        <div style={{ background: 'rgba(255,255,255,0.1)', height: 6, borderRadius: 3, overflow: 'hidden' }}>
                                                            <div style={{
                                                                width: `${getProgress(job)}%`,
                                                                height: '100%',
                                                                background: job.log && job.log.toLowerCase().includes('connection lost') ? 'var(--warning)' : 'var(--primary)',
                                                                transition: 'width 0.5s ease'
                                                            }} />
                                                        </div>
                                                        <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: '0.75em', marginTop: 4, color: 'var(--text-muted)' }}>
                                                            <span>{job.current_speed ? `${formatBytes(job.current_speed)}/s` : '-'}</span>
                                                            <span>{job.eta_seconds ? `~${formatDuration(job.eta_seconds)} left` : '-'}</span>
                                                        </div>
                                                        {/* Show Connection/Error Logs Inline */}
                                                        {job.log && job.log.toLowerCase().includes('connection lost') ? (
                                                            <ConnectionErrorCountdown log={job.log} />
                                                        ) : job.log && (
                                                            /* Generic error fallback */
                                                            <div style={{ fontSize: '0.8em', color: 'var(--warning)', marginTop: 4, fontWeight: 500 }}>
                                                                ⚠ {job.log}
                                                            </div>
                                                        )}
                                                    </div>
                                                ) : job.status === 'completed' ? (
                                                    <div style={{ fontSize: '0.85em', color: 'var(--success)' }}>
                                                        {job.total_bytes > 0 && `Total: ${formatBytes(job.total_bytes)} `}
                                                        {hasFailures && (
                                                            <div style={{ marginTop: 4 }}>
                                                                <span style={{ color: 'var(--error)', marginRight: 6 }}>
                                                                    ⚠ {failedList.length} failed
                                                                </span>
                                                                <button
                                                                    className="text-btn"
                                                                    style={{ textDecoration: 'underline', fontSize: 'inherit', color: 'rgba(255,255,255,0.7)' }}
                                                                    onClick={() => setFailedItems({ jobId: job.id, items: failedList })}
                                                                >
                                                                    View
                                                                </button>
                                                            </div>
                                                        )}
                                                    </div>
                                                ) : (
                                                    <span style={{ fontSize: '0.85em', color: 'var(--text-muted)' }}>{job.log}</span>
                                                )}
                                            </td>
                                            <td style={{ fontSize: '0.9em', color: 'var(--text-muted)' }} data-label="Duration">
                                                {job.started_at && job.completed_at ? (
                                                    formatDuration((new Date(job.completed_at) - new Date(job.started_at)) / 1000)
                                                ) : job.status === 'running' && job.started_at ? (
                                                    'Running...'
                                                ) : '-'}
                                            </td>
                                            <td style={{ fontFamily: 'monospace', wordBreak: 'break-all' }} data-label="Path">
                                                {job.remote_path || '-'}
                                            </td>
                                            <td style={{ fontSize: '0.9em' }} data-label="Started">
                                                {job.started_at ? new Date(job.started_at).toLocaleString() : '-'}
                                            </td>
                                            <td style={{ textAlign: 'right' }}>
                                                <div style={{ display: 'flex', justifyContent: 'flex-end', gap: 6 }}>
                                                    {/* Resume/Pause Button */}
                                                    {(job.status === 'running' || job.status === 'queued') && (
                                                        <button onClick={() => updateJobStatus(job.id, 'pause')} className="btn btn-secondary" style={{ padding: '4px 8px', fontSize: '0.8rem' }} title="Pause">
                                                            <Pause size={14} />
                                                        </button>
                                                    )}
                                                    {(job.status === 'paused' || job.status === 'pausing') && (
                                                        <button onClick={() => updateJobStatus(job.id, 'resume')} className="btn btn-primary" style={{ padding: '4px 8px', fontSize: '0.8rem' }} title="Resume">
                                                            <Play size={14} />
                                                        </button>
                                                    )}

                                                    {/* Prioritize Button - Always Show */}
                                                    <button
                                                        onClick={() => job.status !== 'running' && setPriority(job.id, 10)}
                                                        disabled={['running', 'cancelled', 'completed', 'failed'].includes(job.status)}
                                                        className={`btn ${job.status === 'running' ? 'btn-secondary' : 'btn-secondary'}`}
                                                        style={{
                                                            padding: '4px 8px',
                                                            fontSize: '0.8rem',
                                                            opacity: (['running', 'cancelled', 'completed', 'failed'].includes(job.status)) ? 0.5 : 1,
                                                            cursor: (['running', 'cancelled', 'completed', 'failed'].includes(job.status)) ? 'not-allowed' : 'pointer'
                                                        }}
                                                        title={job.status === 'running' ? "Cannot prioritize running job" : "Prioritize"}
                                                    >
                                                        <ArrowUp size={14} />
                                                    </button>

                                                    {/* Cancel Button */}
                                                    <button
                                                        onClick={() => updateJobStatus(job.id, 'cancel')}
                                                        disabled={!['running', 'queued', 'paused'].includes(job.status)}
                                                        className="btn btn-danger"
                                                        style={{
                                                            padding: '4px 8px',
                                                            fontSize: '0.8rem',
                                                            opacity: (!['running', 'queued', 'paused'].includes(job.status)) ? 0.5 : 1,
                                                            cursor: (!['running', 'queued', 'paused'].includes(job.status)) ? 'not-allowed' : 'pointer'
                                                        }}
                                                        title="Cancel"
                                                    >
                                                        <Trash2 size={14} />
                                                    </button>
                                                </div>
                                            </td>
                                        </tr>
                                    )
                                })}
                                {jobs.length === 0 && (
                                    <tr>
                                        <td colSpan={6} style={{ textAlign: 'center', padding: 20, color: 'var(--text-muted)' }}>
                                            No jobs found matching your criteria.
                                        </td>
                                    </tr>
                                )}
                            </tbody>
                        </table>
                    </div>

                    {/* Pagination Controls */}
                    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '12px 16px', borderTop: '1px solid rgba(255,255,255,0.05)' }}>
                        <div style={{ fontSize: '0.9em', color: 'var(--text-muted)' }}>
                            Showing {Math.min((page - 1) * itemsPerPage + 1, totalItems)} - {Math.min(page * itemsPerPage, totalItems)} of {totalItems}
                        </div>
                        <div style={{ display: 'flex', gap: 8 }}>
                            <button
                                disabled={page === 1}
                                onClick={() => setPage(p => p - 1)}
                                className="btn btn-secondary"
                                style={{ padding: '6px 12px' }}
                            >
                                <ChevronLeft size={16} />
                            </button>
                            <button
                                disabled={page >= totalPages}
                                onClick={() => setPage(p => p + 1)}
                                className="btn btn-secondary"
                                style={{ padding: '6px 12px' }}
                            >
                                <ChevronRight size={16} />
                            </button>
                        </div>
                    </div>
                </div>
            </PullToRefresh>
        </div>
    );
}
