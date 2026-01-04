import { useState, useEffect } from 'react';
import { RefreshCw, Trash2, Power, AlertCircle, CheckCircle, Eye, Play } from 'lucide-react';
import ConfirmationModal from '../components/ConfirmationModal';
import ModalBackdrop from '../components/ModalBackdrop';
import PullToRefresh from '../components/PullToRefresh';
import { createPortal } from 'react-dom';

export default function Dashboard() {
    const [items, setItems] = useState([]);
    const [loading, setLoading] = useState(true);
    const [liveDiffs, setLiveDiffs] = useState({}); // { itemId: { status: 'checking' | 'synced' | 'outdated', ... } }

    const [isRefreshing, setIsRefreshing] = useState(false);

    // Modal state
    const [modalConfig, setModalConfig] = useState({
        isOpen: false,
        title: '',
        message: '',
        onConfirm: () => { },
        confirmLabel: '',
        isDestructive: false
    });

    const fetchItems = async () => {
        try {
            const res = await fetch('http://localhost:3001/api/sync');
            const data = await res.json();
            setItems(data);

            // Initial load check if status missing
            data.forEach(item => {
                if (!liveDiffs[item.id]) {
                    checkLiveStatus(item.id);
                }
            });
        } catch (err) {
            console.error(err);
        } finally {
            setLoading(false);
        }
    };

    const refreshAll = async () => {
        setIsRefreshing(true);
        try {
            // 1. Fetch DB items
            const res = await fetch('http://localhost:3001/api/sync');
            const data = await res.json();
            setItems(data);

            // 2. Force check status for ALL items (active or not)
            await Promise.all(data.map(async (item) => {
                await checkLiveStatus(item.id);
            }));

            // 3. Re-fetch to get updated statuses (e.g. error from local missing)
            await fetchItems();
        } catch (err) {
            console.error(err);
        } finally {
            setIsRefreshing(false);
        }
    };

    const checkLiveStatus = async (id) => {
        setLiveDiffs(prev => ({ ...prev, [id]: { status: 'checking' } }));
        try {
            const res = await fetch(`http://localhost:3001/api/sync/${id}/status`);
            const status = await res.json();
            setLiveDiffs(prev => ({ ...prev, [id]: status }));
        } catch (err) {
            console.error('Diff check failed', err);
            setLiveDiffs(prev => ({ ...prev, [id]: { status: 'error', error: 'Check failed' } }));
        }
    };

    useEffect(() => {
        fetchItems();
        // Removed auto-refresh intervals
    }, []);

    // Removed auto-recheck interval

    const handleSync = (id) => {
        setModalConfig({
            isOpen: true,
            title: 'Start Manual Sync?',
            message: 'Are you sure you want to force a manual sync for this item? This will check the remote server for changes immediately.',
            confirmLabel: 'Sync Now',
            isWarning: true,
            onConfirm: () => performManualSync(id)
        });
    };

    const performManualSync = async (id) => {
        await fetch('http://localhost:3001/api/jobs/manual', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ syncItemId: id })
        });
        fetchItems();
    };

    const toggleActive = async (id, currentIsActive) => {
        // If currently disabled (turning ON), do it immediately
        if (!currentIsActive) {
            await performToggle(id, false);
            return;
        }

        // If currently enabled (turning OFF), show modal
        setModalConfig({
            isOpen: true,
            title: 'Disable Auto-Sync?',
            message: 'Are you sure you want to disable auto-sync for this item? The already synced local files will NOT be deleted.',
            confirmLabel: 'Disable Sync',
            isWarning: true,
            onConfirm: () => performToggle(id, true)
        });
    };

    const performToggle = async (id, current) => {
        await fetch(`http://localhost:3001/api/sync/${id}/toggle`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ active: !current })
        });
        fetchItems();
        setModalConfig({ ...modalConfig, isOpen: false }); // Close modal after action
    };

    const confirmDelete = (id) => {
        setModalConfig({
            isOpen: true,
            title: 'Delete Files?',
            message: 'Are you sure? This will remove the sync configuration AND PERMANENTLY DELETE all local files for this item.',
            confirmLabel: 'Delete Everything',
            isDestructive: true,
            onConfirm: () => performDelete(id)
        });
    };

    const performDelete = async (id) => {
        await fetch(`http://localhost:3001/api/sync/${id}?deleteFiles=true`, { method: 'DELETE' });
        fetchItems();
        setModalConfig({ ...modalConfig, isOpen: false }); // Close modal after action
    };

    const resumeJob = async (jobId) => {
        await fetch(`http://localhost:3001/api/jobs/${jobId}/resume`, { method: 'POST' });
        // Give it a moment to update status
        setTimeout(fetchItems, 500);
    };

    const formatBytes = (bytes) => {
        if (!bytes) return '0 B';
        const k = 1024;
        const i = Math.floor(Math.log(bytes) / Math.log(k));
        const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
        return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i];
    };

    // Diff Modal State
    const [activeDiff, setActiveDiff] = useState(null); // { itemId, files }

    return (
        <div className="animate-enter">
            {/* Diff Viewer Modal */}
            {activeDiff && createPortal(
                <ModalBackdrop onClose={() => setActiveDiff(null)}>
                    {/* ... modal content ... */}
                    <div style={{ padding: 24, paddingBottom: 16, width: '600px', maxWidth: '90vw' }}>
                        <h2 style={{ fontSize: '1.2rem', marginBottom: 8 }}>Sync Differences</h2>
                        <p style={{ color: 'var(--text-muted)', marginBottom: 16, fontSize: '0.9em' }}>
                            The following files on the remote server differ from your local copy:
                        </p>

                        <div style={{
                            background: 'rgba(0,0,0,0.3)',
                            borderRadius: 6,
                            border: '1px solid rgba(255,255,255,0.1)',
                            maxHeight: '60vh',
                            overflowY: 'auto',
                            padding: 0
                        }}>
                            {activeDiff.files.map((file, i) => (
                                <div key={i} style={{
                                    padding: '12px',
                                    borderBottom: '1px solid rgba(255,255,255,0.05)',
                                    display: 'flex',
                                    flexDirection: 'column',
                                    gap: 4
                                }}>
                                    <div style={{ fontFamily: 'monospace', fontSize: '0.9em', color: 'var(--primary)', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                                        <span style={{ marginRight: 12, wordBreak: 'break-all' }}>
                                            {file.remotePath.replace(activeDiff.basePath, '') || file.remotePath}
                                        </span>
                                        {file.reason && ((reason) => {
                                            const colors = {
                                                'missing': 'var(--error)',
                                                'size_mismatch': 'var(--warning)',
                                                'time_mismatch': 'var(--text-muted)'
                                            };
                                            const labels = {
                                                'missing': 'Missing locally',
                                                'size_mismatch': 'Size mismatch',
                                                'time_mismatch': 'Date changed'
                                            };
                                            return (
                                                <span style={{
                                                    fontSize: '0.8rem',
                                                    padding: '2px 6px',
                                                    borderRadius: 4,
                                                    background: 'rgba(255,255,255,0.05)',
                                                    color: colors[reason] || 'var(--text-muted)',
                                                    display: 'flex',
                                                    alignItems: 'center',
                                                    justifyContent: 'center',
                                                    minWidth: 96,
                                                    whiteSpace: 'nowrap',
                                                    flexShrink: 0
                                                }}>
                                                    {labels[reason] || reason}
                                                </span>
                                            );
                                        })(file.reason)}
                                    </div>
                                    <div style={{ fontSize: '0.8em', color: 'var(--text-muted)', display: 'flex', gap: 12 }}>
                                        <span>Size: {formatBytes(file.size)}</span>
                                        <span>Modified: {new Date(file.mtime * 1000).toLocaleString()}</span>
                                    </div>
                                </div>
                            ))}
                        </div>

                        <div style={{ display: 'flex', justifyContent: 'end', marginTop: 20 }}>
                            <button className="btn btn-secondary" onClick={() => setActiveDiff(null)}>
                                Close
                            </button>
                        </div>
                    </div>
                </ModalBackdrop>,
                document.body
            )
            }

            {/* Confirmation Modal */}
            <ConfirmationModal
                isOpen={modalConfig.isOpen}
                onClose={() => setModalConfig({ ...modalConfig, isOpen: false })}
                onConfirm={modalConfig.onConfirm}
                title={modalConfig.title}
                message={modalConfig.message}
                confirmLabel={modalConfig.confirmLabel}
                isDestructive={modalConfig.isDestructive}
                isWarning={modalConfig.isWarning}
            />

            <PullToRefresh onRefresh={refreshAll}>
                <div className="header">
                    <h1 className="page-title">Dashboard</h1>
                    <p style={{ color: 'var(--text-muted)' }}>Overview of your synced folders and files.</p>
                </div>

                <div className="card">
                    <div className="hide-mobile" style={{ display: 'flex', justifyContent: 'flex-end', padding: '12px 16px', borderBottom: '1px solid rgba(255,255,255,0.05)' }}>
                        <button className="btn btn-secondary" onClick={refreshAll} disabled={isRefreshing}>
                            <RefreshCw size={16} className={isRefreshing ? 'spin' : ''} />
                            <span style={{ marginLeft: 8 }}>Refresh Status</span>
                        </button>
                    </div>
                    {loading ? <p>Loading...</p> : items.length === 0 ? (
                        <div style={{ textAlign: 'center', padding: '40px', color: 'var(--text-muted)' }}>
                            <p>No sync items configured. Go to <b>Remote Browser</b> to add one.</p>
                        </div>
                    ) : (
                        <div className="table-wrap">
                            <table className="fixed-table">
                                <thead>
                                    <tr>
                                        <th style={{ width: 60 }}>Type</th>
                                        <th style={{ width: '25%' }}>Remote Path</th>
                                        <th style={{ width: '25%' }}>Local Path</th>
                                        <th style={{ width: 120 }}>Status</th>
                                        <th style={{ width: 160 }}>Last Synced</th>
                                        <th style={{ textAlign: 'right', width: 190 }}>Actions</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    {items.map(item => (
                                        <tr key={item.id} style={{ opacity: (liveDiffs[item.id]?.status === 'synced' || liveDiffs[item.id]?.status === 'outdated' || (item.active && liveDiffs[item.id]?.status !== 'local_missing')) ? 1 : 0.5 }}>
                                            <td>{item.type === 'folder' ? '📁' : '📄'}</td>
                                            <td style={{ fontFamily: 'monospace' }} data-label="Remote Path">{item.remote_path}</td>
                                            <td style={{ fontFamily: 'monospace', color: 'var(--text-muted)' }} data-label="Local Path">{item.local_path}</td>
                                            <td data-label="Status">
                                                {(() => {
                                                    const isLocalMissing = liveDiffs[item.id]?.status === 'local_missing';
                                                    const isOutdated = liveDiffs[item.id]?.status === 'outdated';
                                                    const activeJob = item.activeJob;

                                                    let displayStatus = item.status;
                                                    let displayClass = item.status;

                                                    if (activeJob) {
                                                        // running, queued, pausing
                                                        if (activeJob.status === 'queued') {
                                                            displayStatus = 'Queued';
                                                            displayClass = 'queued';
                                                        } else if (activeJob.status === 'running') {
                                                            displayStatus = 'Running';
                                                            displayClass = 'running';
                                                        } else {
                                                            displayStatus = 'Pending';
                                                            displayClass = 'pending';
                                                        }
                                                    } else if (isLocalMissing) {
                                                        displayStatus = 'error';
                                                        displayClass = 'error';
                                                    } else if (isOutdated) {
                                                        displayStatus = 'Out of Sync';
                                                        displayClass = 'out-of-sync';
                                                    }

                                                    const displayError = item.error_message || (isLocalMissing ? liveDiffs[item.id].error : null);

                                                    return (
                                                        <>
                                                            <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                                                                <span className={`status-badge status-${displayClass.toLowerCase().replace(/ /g, '-')}`}>
                                                                    {displayStatus}
                                                                </span>
                                                            </div>
                                                            {displayError && (
                                                                <div style={{ fontSize: '0.8em', color: 'var(--error)', marginTop: 4 }}>
                                                                    {displayError}
                                                                </div>
                                                            )}
                                                            {/* Live Status Indicator - Always show regardless of Active status */}
                                                            <div style={{ fontSize: '0.75em', marginTop: 6, display: 'flex', alignItems: 'center', gap: 4 }}>
                                                                {(!liveDiffs[item.id] || liveDiffs[item.id].status === 'checking') && (
                                                                    <span style={{ color: 'var(--text-muted)' }}>Checking remote...</span>
                                                                )}
                                                                {liveDiffs[item.id]?.status === 'synced' && (
                                                                    <span style={{ color: 'var(--success)' }}>✔ Up to date</span>
                                                                )}
                                                                {liveDiffs[item.id]?.status === 'outdated' && (
                                                                    activeJob ? (
                                                                        activeJob.status === 'paused' ? (

                                                                            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', width: '100%', gap: 6 }}>
                                                                                <span style={{ color: 'var(--warning)', whiteSpace: 'nowrap' }}>⚠ Sync paused</span>
                                                                                <button
                                                                                    onClick={() => resumeJob(activeJob.id)}
                                                                                    className="text-btn"
                                                                                    style={{ color: 'var(--primary)', display: 'flex', alignItems: 'center', gap: 4, textDecoration: 'underline' }}
                                                                                >
                                                                                    <Play size={14} /> Resume
                                                                                </button>
                                                                            </div>
                                                                        ) : (
                                                                            <span style={{ color: 'var(--success)' }}>
                                                                                {activeJob.status === 'queued' ? '⧖ Waiting for other sync to finish...' : '⚠ Sync running...'}
                                                                            </span>
                                                                        )
                                                                    ) : (

                                                                        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', width: '100%', gap: 6 }}>
                                                                            <span style={{ color: 'var(--warning)' }}>⚠ Remote changed ({liveDiffs[item.id].diffCount} files)</span>
                                                                            <button
                                                                                className="text-btn"
                                                                                style={{ textDecoration: 'underline', fontSize: 'inherit', color: 'rgba(255,255,255,0.7)', padding: 0 }}
                                                                                onClick={() => setActiveDiff({ itemId: item.id, files: liveDiffs[item.id].diffFiles || [], basePath: item.remote_path })}
                                                                            >
                                                                                View
                                                                            </button>
                                                                        </div>
                                                                    )
                                                                )}
                                                                {liveDiffs[item.id]?.status === 'error' && (
                                                                    <span style={{ color: 'var(--error)' }}>⚠ Check failed</span>
                                                                )}
                                                            </div>
                                                        </>
                                                    );
                                                })()}
                                            </td>
                                            <td style={{ fontSize: '0.9em' }} data-label="Last Synced">
                                                {item.last_synced_at ? new Date(item.last_synced_at).toLocaleString() : '-'}
                                            </td>
                                            <td style={{ textAlign: 'right' }}>
                                                <div style={{ display: 'flex', justifyContent: 'flex-end', gap: 8 }}>
                                                    <button
                                                        onClick={() => handleSync(item.id)}
                                                        className="btn btn-secondary"
                                                        title="Sync Now"
                                                        disabled={item.status === 'running' || item.status === 'syncing'}
                                                    >
                                                        <RefreshCw size={16} />
                                                    </button>
                                                    <button
                                                        onClick={() => toggleActive(item.id, item.active)}
                                                        className="btn btn-secondary"
                                                        title={item.active ? "Disable Auto-Sync" : "Enable Auto-Sync"}
                                                    >
                                                        <Power size={16} color={item.active ? 'var(--success)' : 'var(--text-muted)'} />
                                                    </button>
                                                    <button onClick={() => confirmDelete(item.id)} className="btn btn-danger" title="Remove">
                                                        <Trash2 size={16} />
                                                    </button>
                                                </div>
                                            </td>
                                        </tr>
                                    ))}
                                </tbody>
                            </table>
                        </div>
                    )}
                </div>
            </PullToRefresh>
        </div>
    );
}
