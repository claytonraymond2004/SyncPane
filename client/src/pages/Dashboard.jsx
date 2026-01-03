import { useState, useEffect } from 'react';
import { RefreshCw, Trash2, Power, AlertCircle, CheckCircle } from 'lucide-react';
import ConfirmationModal from '../components/ConfirmationModal';

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

            // Initial load check (only if missing)
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

    return (
        <div className="animate-enter">
            <div className="header">
                <h1 className="page-title">Dashboard</h1>
                <p style={{ color: 'var(--text-muted)' }}>Overview of your synced folders and files.</p>
            </div>

            <div className="card">
                <div style={{ display: 'flex', justifyContent: 'flex-end', padding: '12px 16px', borderBottom: '1px solid rgba(255,255,255,0.05)' }}>
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
                        <table>
                            <thead>
                                <tr>
                                    <th style={{ width: 50 }}>Type</th>
                                    <th>Remote Path</th>
                                    <th>Local Path</th>
                                    <th>Status</th>
                                    <th>Last Synced</th>
                                    <th style={{ textAlign: 'right' }}>Actions</th>
                                </tr>
                            </thead>
                            <tbody>
                                {items.map(item => (
                                    <tr key={item.id} style={{ opacity: (liveDiffs[item.id]?.status === 'synced' || (item.active && liveDiffs[item.id]?.status !== 'local_missing')) ? 1 : 0.5 }}>
                                        <td>{item.type === 'folder' ? '📁' : '📄'}</td>
                                        <td style={{ fontFamily: 'monospace' }}>{item.remote_path}</td>
                                        <td style={{ fontFamily: 'monospace', color: 'var(--text-muted)' }}>{item.local_path}</td>
                                        <td>
                                            {(() => {
                                                const isLocalMissing = liveDiffs[item.id]?.status === 'local_missing';
                                                const displayStatus = isLocalMissing ? 'error' : item.status;
                                                const displayError = item.error_message || (isLocalMissing ? liveDiffs[item.id].error : null);

                                                return (
                                                    <>
                                                        <span className={`status-badge status-${displayStatus}`}>
                                                            {displayStatus}
                                                        </span>
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
                                                                <span style={{ color: 'var(--warning)' }}>⚠ Remote changed ({liveDiffs[item.id].diffCount} files)</span>
                                                            )}
                                                            {liveDiffs[item.id]?.status === 'error' && (
                                                                <span style={{ color: 'var(--error)' }}>⚠ Check failed</span>
                                                            )}
                                                        </div>
                                                    </>
                                                );
                                            })()}
                                        </td>
                                        <td style={{ fontSize: '0.9em' }}>
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
        </div>
    );
}
