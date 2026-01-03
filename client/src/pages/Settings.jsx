import { useState, useEffect } from 'react';
import { createPortal } from 'react-dom';
import { Save, Plus, X, Folder, FileText, ArrowLeft, Activity, Server, Globe, Settings as SettingsIcon } from 'lucide-react';
import ConfirmationModal from '../components/ConfirmationModal';
import ToastContainer from '../components/Toast';

export default function Settings() {
    // --- Global Settings State ---
    const [globalConfig, setGlobalConfig] = useState({
        sync_schedule: '0 * * * *',
        global_sync_enabled: true,
        connection_timeout_minutes: 60
    });

    // Explicit UI mode state for Custom Schedule
    const standardSchedules = ['* * * * *', '*/15 * * * *', '0 * * * *', '0 */6 * * *', '0 0 * * *'];
    const [isCustomMode, setIsCustomMode] = useState(false);

    const [globalSaving, setGlobalSaving] = useState(false);

    // --- Toast State ---
    const [toasts, setToasts] = useState([]);

    const addToast = (message, type = 'success', duration = 5000) => {
        const id = Date.now();
        setToasts(prev => [...prev, { id, message, type, duration }]);
    };

    const removeToast = (id) => {
        setToasts(prev => prev.filter(t => t.id !== id));
    };

    // --- SSH Config State ---
    const [sshConfig, setSshConfig] = useState({
        host: '', port: 22, username: '', password: '', privateKey: ''
    });
    const [hasPass, setHasPass] = useState(false);
    // sshStatus removed in favor of Toast

    // --- Sync Locations State ---
    const [locations, setLocations] = useState([]);
    const [newLoc, setNewLoc] = useState('');
    const [browserOpen, setBrowserOpen] = useState(false);

    // --- Common State ---
    const [loading, setLoading] = useState(true);
    const [modalConfig, setModalConfig] = useState({
        isOpen: false,
        title: '',
        message: '',
        onConfirm: () => { },
        confirmLabel: '',
        isWarning: false
    });
    const [confirmSaveOpen, setConfirmSaveOpen] = useState(false);
    const [saveErrorMessage, setSaveErrorMessage] = useState('');

    useEffect(() => {
        Promise.all([
            fetch('http://localhost:3001/api/settings').then(res => res.json()),
            fetch('http://localhost:3001/api/config').then(res => res.json()),
            fetch('http://localhost:3001/api/sync-locations').then(res => res.json())
        ]).then(([settingsData, sshData, locData]) => {
            setGlobalConfig(settingsData);
            // check if loaded schedule is non-standard
            setIsCustomMode(!standardSchedules.includes(settingsData.sync_schedule));

            setSshConfig(prev => ({ ...prev, ...sshData, password: '', privateKey: '' }));
            setHasPass(sshData.hasPassword);
            setLocations(locData);
        }).catch(err => console.error(err))
            .finally(() => setLoading(false));
    }, []);

    // --- Global Settings Handlers ---
    const handleSaveGlobal = async (overrideConfig) => {
        const configToSave = overrideConfig || globalConfig;
        setGlobalSaving(true);
        try {
            await fetch('http://localhost:3001/api/settings', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(configToSave)
            });
            // Update local state if we saved an override
            if (overrideConfig) setGlobalConfig(overrideConfig);

            addToast('Settings saved successfully!', 'success');
        } catch (err) {
            addToast('Failed to save settings: ' + err.message, 'error');
        } finally {
            setGlobalSaving(false);
        }
    };

    const handleToggleGlobal = () => {
        const newValue = !globalConfig.global_sync_enabled;

        if (!newValue) {
            setModalConfig({
                isOpen: true,
                title: 'Disable Global Sync?',
                message: 'This will stop ALL scheduled sync jobs. Manual syncs will still work.',
                confirmLabel: 'Disable All Syncs',
                isWarning: true,
                onConfirm: () => {
                    handleSaveGlobal({ ...globalConfig, global_sync_enabled: false });
                    setModalConfig(prev => ({ ...prev, isOpen: false }));
                }
            });
        } else {
            handleSaveGlobal({ ...globalConfig, global_sync_enabled: true });
        }
    };

    // --- SSH Config Handlers ---
    const handleTestConnection = async (e) => {
        e.preventDefault();
        addToast('Testing connection...', 'warning', 2000);
        try {
            const res = await fetch('http://localhost:3001/api/config/test', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(sshConfig)
            });
            const data = await res.json();
            if (data.success) addToast('Connection successful!', 'success');
            else addToast('Connection failed: ' + data.error, 'error');
        } catch (err) {
            addToast('Connection error: ' + err.message, 'error');
        }
    };

    const handleSaveSSH = async (e, skipTest = false) => {
        if (e) e.preventDefault();
        if (!skipTest) addToast('Verifying connection...', 'warning', 2000);

        try {
            const res = await fetch('http://localhost:3001/api/config', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ ...sshConfig, skipTest })
            });
            const data = await res.json();

            if (data.success) {
                addToast(data.message, 'success');
            } else {
                if (!skipTest && data.type === 'CONNECTION_FAILED') {
                    setSaveErrorMessage(data.error);
                    setConfirmSaveOpen(true);
                } else {
                    addToast('Error saving: ' + data.error, 'error');
                }
            }
        } catch (err) {
            addToast('Error: ' + err.message, 'error');
        }
    };

    // --- Locations Handlers ---
    const addLocation = async (pathToAdd) => {
        const path = pathToAdd || newLoc;
        if (!path) return;

        await fetch('http://localhost:3001/api/sync-locations', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ path: path, label: '' })
        });
        setNewLoc('');
        fetchLocations();
        addToast('Path added to favorites', 'success');
    };

    const fetchLocations = () => {
        fetch('http://localhost:3001/api/sync-locations').then(res => res.json()).then(setLocations);
    };

    const deleteLocation = (id) => {
        setModalConfig({
            isOpen: true,
            title: 'Remove Favorite Path?',
            message: 'Are you sure you want to remove this favorite local path?',
            confirmLabel: 'Remove',
            isDestructive: true,
            onConfirm: async () => {
                await fetch(`http://localhost:3001/api/sync-locations/${id}`, { method: 'DELETE' });
                fetchLocations();
                setModalConfig(prev => ({ ...prev, isOpen: false }));
            }
        });
    };

    return (
        <div className="animate-enter" style={{ maxWidth: 800, paddingBottom: 40 }}>
            {/* Modal for SSH Save Anyways */}
            <ConfirmationModal
                isOpen={confirmSaveOpen}
                onClose={() => setConfirmSaveOpen(false)}
                onConfirm={() => handleSaveSSH(null, true)}
                title="Connection Test Failed"
                message={`The connection test failed with the following error:\n\n${saveErrorMessage}\n\nDo you want to save this configuration anyway?`}
                confirmLabel="Save Anyway"
                isWarning={true}
            />

            {/* Modal for Global Toggle */}
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

            <div className="header">
                <h1 className="page-title">Settings</h1>
                <p style={{ color: 'var(--text-muted)' }}>Configure global application behavior and connections.</p>
            </div>

            {loading ? <p>Loading...</p> : (
                <div style={{ display: 'flex', flexDirection: 'column', gap: 32 }}>

                    {/* --- SECTION 1: GLOBAL POLICY --- */}
                    <div className="card">
                        <h2 style={{ marginBottom: 24, fontSize: '1.2rem', display: 'flex', alignItems: 'center', gap: 10 }}>
                            <SettingsIcon size={20} color="var(--primary)" /> Global Policy
                        </h2>

                        <div style={{ display: 'flex', flexDirection: 'column', gap: 24 }}>
                            {/* Toggle */}
                            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', paddingBottom: 24, borderBottom: '1px solid rgba(255,255,255,0.05)' }}>
                                <div>
                                    <h3 style={{ fontSize: '1.1rem', marginBottom: 4 }}>Global Sync</h3>
                                    <p style={{ color: 'var(--text-muted)', fontSize: '0.9rem' }}>Master switch for all scheduled operations.</p>
                                </div>
                                <div
                                    onClick={handleToggleGlobal}
                                    style={{
                                        width: 48, height: 28,
                                        background: globalConfig.global_sync_enabled ? 'var(--primary)' : 'rgba(255,255,255,0.1)',
                                        borderRadius: 14,
                                        position: 'relative',
                                        cursor: 'pointer',
                                        transition: 'background 0.2s'
                                    }}
                                >
                                    <div style={{
                                        width: 20, height: 20,
                                        background: 'white',
                                        borderRadius: '50%',
                                        position: 'absolute',
                                        top: 4,
                                        left: globalConfig.global_sync_enabled ? 24 : 4,
                                        transition: 'left 0.2s',
                                        boxShadow: '0 2px 4px rgba(0,0,0,0.2)'
                                    }} />
                                </div>
                            </div>

                            {/* Frequency */}
                            <div>
                                <h3 style={{ fontSize: '1.1rem', marginBottom: 8 }}>Sync Frequency</h3>
                                <p style={{ color: 'var(--text-muted)', fontSize: '0.9rem', marginBottom: 12 }}>How often should the system check for changes?</p>

                                <select
                                    className="input"
                                    value={isCustomMode ? 'custom' : globalConfig.sync_schedule}
                                    onChange={(e) => {
                                        if (e.target.value === 'custom') {
                                            setIsCustomMode(true);
                                        } else {
                                            setIsCustomMode(false);
                                            setGlobalConfig({ ...globalConfig, sync_schedule: e.target.value });
                                        }
                                    }}
                                    style={{ width: '100%', padding: 12, borderRadius: 'var(--radius-md)', background: 'rgba(0,0,0,0.2)', border: '1px solid rgba(255,255,255,0.1)', color: 'white', marginBottom: isCustomMode ? 12 : 0 }}
                                    disabled={!globalConfig.global_sync_enabled}
                                >
                                    <option value="* * * * *">Every Minute (Debug)</option>
                                    <option value="*/15 * * * *">Every 15 Minutes</option>
                                    <option value="0 * * * *">Every Hour</option>
                                    <option value="0 */6 * * *">Every 6 Hours</option>
                                    <option value="0 0 * * *">Daily (Midnight)</option>
                                    <option value="custom">Custom Cron Expression</option>
                                </select>

                                {isCustomMode && (
                                    <div>
                                        <input
                                            type="text"
                                            value={globalConfig.sync_schedule}
                                            onChange={(e) => setGlobalConfig({ ...globalConfig, sync_schedule: e.target.value })}
                                            placeholder="*/5 * * * *"
                                            style={{ width: '100%', padding: 12, borderRadius: 'var(--radius-md)', background: 'rgba(0,0,0,0.2)', border: '1px solid rgba(255,255,255,0.1)', color: 'white', fontFamily: 'monospace' }}
                                        />
                                        <p style={{ marginTop: 8, fontSize: '0.8rem', color: 'var(--text-muted)' }}>
                                            Standard Cron format: <code>minute hour day(month) month day(week)</code>
                                        </p>
                                    </div>
                                )}
                            </div>

                            {/* Timeout */}
                            <div>
                                <h3 style={{ fontSize: '1.1rem', marginBottom: 8 }}>Connection Timeout</h3>
                                <p style={{ color: 'var(--text-muted)', fontSize: '0.9rem', marginBottom: 12 }}>
                                    How long to retry if the connection is lost (in minutes).
                                </p>
                                <input
                                    type="number"
                                    min="1"
                                    value={globalConfig.connection_timeout_minutes}
                                    onChange={(e) => setGlobalConfig({ ...globalConfig, connection_timeout_minutes: parseInt(e.target.value) || 1 })}
                                    style={{ width: '100%', padding: 12, borderRadius: 'var(--radius-md)', background: 'rgba(0,0,0,0.2)', border: '1px solid rgba(255,255,255,0.1)', color: 'white' }}
                                    disabled={!globalConfig.global_sync_enabled}
                                />
                            </div>

                            <div style={{ paddingTop: 12, display: 'flex', alignItems: 'center', justifyContent: 'flex-end', gap: 16 }}>
                                <button className="btn btn-primary" onClick={() => handleSaveGlobal()} disabled={globalSaving}>
                                    {globalSaving ? <span className="spin"><Save size={16} /></span> : <Save size={16} />}
                                    <span>Save Policy</span>
                                </button>
                            </div>
                        </div>
                    </div>

                    {/* --- SECTION 2: REMOTE SERVER --- */}
                    <div className="card">
                        <h2 style={{ marginBottom: 24, fontSize: '1.2rem', display: 'flex', alignItems: 'center', gap: 10 }}>
                            <Server size={20} color="var(--primary)" /> Remote Server (SSH)
                        </h2>
                        <form onSubmit={(e) => handleSaveSSH(e)} style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 16 }}>
                            <div style={{ gridColumn: 'span 2' }}>
                                <label style={{ display: 'block', marginBottom: 8, fontSize: '0.9em', color: 'var(--text-muted)' }}>Host / IP</label>
                                <input
                                    className="input"
                                    type="text"
                                    value={sshConfig.host}
                                    onChange={e => setSshConfig({ ...sshConfig, host: e.target.value })}
                                    placeholder="e.g. 192.168.1.100"
                                    style={{ width: '100%', padding: '12px', borderRadius: 8, border: '1px solid rgba(255,255,255,0.1)', background: 'rgba(0,0,0,0.2)', color: 'white' }}
                                />
                            </div>
                            <div>
                                <label style={{ display: 'block', marginBottom: 8, fontSize: '0.9em', color: 'var(--text-muted)' }}>Port</label>
                                <input
                                    type="number"
                                    value={sshConfig.port}
                                    onChange={e => setSshConfig({ ...sshConfig, port: parseInt(e.target.value) })}
                                    style={{ width: '100%', padding: '12px', borderRadius: 8, border: '1px solid rgba(255,255,255,0.1)', background: 'rgba(0,0,0,0.2)', color: 'white' }}
                                />
                            </div>
                            <div>
                                <label style={{ display: 'block', marginBottom: 8, fontSize: '0.9em', color: 'var(--text-muted)' }}>Username</label>
                                <input
                                    type="text"
                                    value={sshConfig.username}
                                    onChange={e => setSshConfig({ ...sshConfig, username: e.target.value })}
                                    style={{ width: '100%', padding: '12px', borderRadius: 8, border: '1px solid rgba(255,255,255,0.1)', background: 'rgba(0,0,0,0.2)', color: 'white' }}
                                />
                            </div>
                            <div style={{ gridColumn: 'span 2' }}>
                                <label style={{ display: 'block', marginBottom: 8, fontSize: '0.9em', color: 'var(--text-muted)' }}>
                                    Password {hasPass && <span style={{ color: 'var(--success)' }}>(Stored)</span>}
                                </label>
                                <input
                                    type="password"
                                    value={sshConfig.password}
                                    onChange={e => setSshConfig({ ...sshConfig, password: e.target.value })}
                                    placeholder={hasPass ? "Leave empty to keep existing" : "Enter password"}
                                    style={{ width: '100%', padding: '12px', borderRadius: 8, border: '1px solid rgba(255,255,255,0.1)', background: 'rgba(0,0,0,0.2)', color: 'white' }}
                                />
                            </div>
                            <div style={{ gridColumn: 'span 2', marginTop: 16, display: 'flex', alignItems: 'center', gap: 12 }}>
                                <button
                                    type="button"
                                    onClick={handleTestConnection}
                                    className="btn btn-secondary"
                                    style={{ marginRight: 'auto' }}
                                >
                                    <Activity size={18} /> Test Connection
                                </button>
                                <button type="submit" className="btn btn-primary">
                                    <Save size={18} /> Save SSH Config
                                </button>
                            </div>
                        </form>
                    </div>

                    {/* --- SECTION 3: LOCAL PATHS --- */}
                    <div className="card">
                        <h2 style={{ marginBottom: 24, fontSize: '1.2rem', display: 'flex', alignItems: 'center', gap: 10 }}>
                            <Folder size={20} color="var(--primary)" /> Favorite Paths
                        </h2>
                        <div style={{ display: 'flex', gap: 12, marginBottom: 16 }}>
                            <input
                                type="text"
                                value={newLoc}
                                onChange={e => setNewLoc(e.target.value)}
                                placeholder="/app/sync/Downloads"
                                style={{ flex: 1, padding: '12px', borderRadius: 8, border: '1px solid rgba(255,255,255,0.1)', background: 'rgba(0,0,0,0.2)', color: 'white' }}
                            />
                            <button onClick={() => setBrowserOpen(true)} className="btn btn-secondary" style={{ whiteSpace: 'nowrap' }}>
                                <Folder size={18} /> Browse
                            </button>
                            <button onClick={addLocation} className="btn btn-primary"><Plus size={18} /></button>
                        </div>
                        <div>
                            {locations.map(loc => (
                                <div key={loc.id} style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '12px', borderBottom: '1px solid rgba(255,255,255,0.05)' }}>
                                    <span style={{ fontFamily: 'monospace' }}>{loc.path}</span>
                                    <button
                                        onClick={() => deleteLocation(loc.id)}
                                        className="btn-icon"
                                        style={{ color: 'var(--text-muted)', padding: 4 }}
                                        title="Remove Path"
                                    >
                                        <X size={16} />
                                    </button>
                                </div>
                            ))}
                        </div>
                    </div>
                </div>
            )}

            {/* Local Browser Modal */}
            {browserOpen && createPortal(
                <LocalBrowserModal
                    onSelect={(path) => {
                        addLocation(path);
                        setBrowserOpen(false);
                    }}
                    onClose={() => setBrowserOpen(false)}
                />,
                document.body
            )}

            <ToastContainer toasts={toasts} removeToast={removeToast} />
        </div>
    );
}

function LocalBrowserModal({ onSelect, onClose }) {
    const [path, setPath] = useState('/app'); // Start at typical docker app root
    const [files, setFiles] = useState([]);
    const [loading, setLoading] = useState(false);

    useEffect(() => {
        const handleEsc = (e) => {
            if (e.key === 'Escape') onClose();
        };
        window.addEventListener('keydown', handleEsc);
        return () => window.removeEventListener('keydown', handleEsc);
    }, [onClose]);

    useEffect(() => {
        fetchFiles(path);
    }, [path]);

    const fetchFiles = async (p) => {
        setLoading(true);
        try {
            const res = await fetch(`http://localhost:3001/api/local/list?path=${encodeURIComponent(p)}`);
            const data = await res.json();
            if (res.ok) setFiles(data);
            else console.error(data);
        } catch (err) {
            console.error(err);
        } finally {
            setLoading(false);
        }
    };

    const handleUp = () => {
        if (path === '/') return;
        const parent = path.substring(0, path.lastIndexOf('/')) || '/';
        setPath(parent);
    };

    return (
        <div
            onClick={(e) => {
                if (e.target === e.currentTarget) onClose();
            }}
            style={{
                position: 'fixed', top: 0, left: 0, right: 0, bottom: 0,
                background: 'rgba(0,0,0,0.8)', backdropFilter: 'blur(4px)',
                display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 9999
            }}
        >
            <div className="card" style={{ width: 600, maxWidth: '90%', maxHeight: '80vh', display: 'flex', flexDirection: 'column' }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 16 }}>
                    <h3>Select Folder</h3>
                    <button onClick={onClose}><X size={20} /></button>
                </div>

                <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 12, padding: '8px', background: 'rgba(0,0,0,0.2)', borderRadius: 6 }}>
                    <button onClick={handleUp} disabled={path === '/'} style={{ opacity: path === '/' ? 0.3 : 1 }}>
                        <ArrowLeft size={16} />
                    </button>
                    <code style={{ fontSize: '0.9em', wordBreak: 'break-all' }}>{path}</code>
                </div>

                <div style={{ flex: 1, overflowY: 'auto', minHeight: 300, border: '1px solid rgba(255,255,255,0.05)', borderRadius: 6 }}>
                    {loading ? <div style={{ padding: 20 }}>Loading...</div> : (
                        <table style={{ width: '100%', borderCollapse: 'collapse' }}>
                            <tbody>
                                {files.map(file => (
                                    <tr key={file.name} style={{ borderBottom: '1px solid rgba(255,255,255,0.05)' }}>
                                        <td style={{ padding: '8px 12px', width: 40 }}>
                                            {file.type === 'folder' ? <Folder color="var(--primary)" size={18} /> : <FileText color="var(--text-muted)" size={18} />}
                                        </td>
                                        <td style={{ padding: '8px 12px' }}>
                                            {file.type === 'folder' ? (
                                                <button onClick={() => setPath(file.path)} style={{ textAlign: 'left', fontWeight: 500, color: 'var(--text-main)', width: '100%' }}>
                                                    {file.name}
                                                </button>
                                            ) : (
                                                <span style={{ color: 'var(--text-muted)' }}>{file.name}</span>
                                            )}
                                        </td>
                                        <td style={{ textAlign: 'right', padding: '8px' }}>
                                            {file.type === 'folder' && (
                                                <button
                                                    onClick={() => onSelect(file.path)}
                                                    className="btn btn-secondary"
                                                    style={{ fontSize: '0.75rem', padding: '4px 8px' }}
                                                >
                                                    Select
                                                </button>
                                            )}
                                        </td>
                                    </tr>
                                ))}
                            </tbody>
                        </table>
                    )}
                </div>
            </div>
        </div>
    );
}
