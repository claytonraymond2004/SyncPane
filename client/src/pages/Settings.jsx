import { useState, useEffect } from 'react';
import { createPortal } from 'react-dom';
import { Save, Plus, X, Folder, FileText, ArrowLeft, Activity, Server, Globe, Settings as SettingsIcon, Trash2, AlertTriangle, Wrench } from 'lucide-react';
import ConfirmationModal from '../components/ConfirmationModal';
import ToastContainer from '../components/Toast';
import LocalBrowser from '../components/LocalBrowser';
import { useNavigate } from 'react-router-dom';

// Helper to normalize config for comparison (ensures key order/subset)
const normalizeGlobal = (c) => ({
    sync_schedule: c.sync_schedule,
    global_sync_enabled: c.global_sync_enabled,
    connection_timeout_minutes: c.connection_timeout_minutes
});

const normalizeSSH = (c) => ({
    host: c.host,
    port: c.port,
    username: c.username,
    password: c.password,
    privateKey: c.privateKey
});

export default function Settings() {
    const navigate = useNavigate();
    // --- Global Settings State ---
    const [globalConfig, setGlobalConfig] = useState({
        sync_schedule: '0 * * * *',
        global_sync_enabled: true,
        connection_timeout_minutes: 60
    });
    const [originalGlobalConfig, setOriginalGlobalConfig] = useState(null);

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
    const [originalSshConfig, setOriginalSshConfig] = useState(null);
    const [hasPass, setHasPass] = useState(false);
    const [hasKey, setHasKey] = useState(false);
    const [isEnv, setIsEnv] = useState(false);
    const [authMethod, setAuthMethod] = useState('password'); // 'password' | 'key'

    // sshStatus removed in favor of Toast

    // --- Sync Locations State ---
    const [locations, setLocations] = useState([]);
    const [newLoc, setNewLoc] = useState('');
    const [browserOpen, setBrowserOpen] = useState(false);

    // --- Maintenance State ---
    const [purgeDays, setPurgeDays] = useState(7);


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
            setOriginalGlobalConfig(settingsData);

            // check if loaded schedule is non-standard
            setIsCustomMode(!standardSchedules.includes(settingsData.sync_schedule));

            // Merge loaded SSH data with default structure to ensure consistency
            // Use undefined for secrets so they are omitted from JSON unless changed
            const loadedSsh = { ...sshConfig, ...sshData, password: undefined, privateKey: undefined };

            setSshConfig(loadedSsh);
            setOriginalSshConfig(loadedSsh);

            setHasPass(sshData.hasPassword);
            setHasKey(sshData.hasKey);
            setIsEnv(sshData.isEnv);
            setLocations(locData);

            // Determine initial auth method
            if (sshData.hasKey) {
                setAuthMethod('key');
            } else {
                setAuthMethod('password');
            }
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
            setOriginalGlobalConfig(configToSave);

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
                setOriginalSshConfig(sshConfig);
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

    const handleKeyUpload = (e) => {
        const file = e.target.files[0];
        if (!file) return;

        const reader = new FileReader();
        reader.onload = (loadEvent) => {
            setSshConfig(prev => ({ ...prev, privateKey: loadEvent.target.result }));
            addToast('Private key loaded for upload', 'success');
        };
        reader.readAsText(file);
    };

    const handleClearKey = () => {
        setSshConfig(prev => ({ ...prev, privateKey: '' }));
        setHasKey(false); // Optimistic UI update
        addToast('Private key cleared (save to persist)', 'info');
    };

    const handleClearPassword = () => {
        setSshConfig(prev => ({ ...prev, password: '' }));
        setHasPass(false); // Optimistic UI update
        addToast('Password cleared (save to persist)', 'info');
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

    // --- Maintenance Handlers ---
    const handlePurgeHistory = async () => {
        try {
            const res = await fetch('http://localhost:3001/api/maintenance/purge-jobs', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ days: purgeDays })
            });
            const data = await res.json();
            if (data.success) {
                addToast(data.message, 'success');
            } else {
                addToast('Purge failed: ' + data.error, 'error');
            }
        } catch (err) {
            addToast('Purge error: ' + err.message, 'error');
        }
    };

    const handleResetApp = () => {
        setModalConfig({
            isOpen: true,
            title: 'Reset Application?',
            message: 'This will DELETE ALL data including:\n- Sync Items\n- Job History\n- Settings & Credentials\n- Favorite Paths\n\nThe application will be returned to a fresh state. This action cannot be undone.',
            confirmLabel: 'RESET EVERYTHING',
            isDestructive: true,
            isWarning: true,
            onConfirm: async () => {
                try {
                    const res = await fetch('http://localhost:3001/api/maintenance/reset-app', { method: 'POST' });
                    const data = await res.json();
                    if (data.success) {
                        addToast('Application reset successfully. Reloading...', 'success');
                        setTimeout(() => {
                            window.location.reload();
                        }, 1500);
                    } else {
                        addToast('Reset failed: ' + data.error, 'error');
                    }
                } catch (err) {
                    addToast('Reset error: ' + err.message, 'error');
                }
                setModalConfig(prev => ({ ...prev, isOpen: false }));
            }
        });
    };

    // Computed changes
    const hasGlobalChanges = originalGlobalConfig && JSON.stringify(normalizeGlobal(globalConfig)) !== JSON.stringify(normalizeGlobal(originalGlobalConfig));
    const hasSSHChanges = originalSshConfig && JSON.stringify(normalizeSSH(sshConfig)) !== JSON.stringify(normalizeSSH(originalSshConfig));

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
                                    <h3 style={{ fontSize: '1.1rem', marginBottom: 4 }}>Scheduled Sync</h3>
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
                                <button
                                    className="btn btn-primary"
                                    onClick={() => handleSaveGlobal()}
                                    disabled={globalSaving || !hasGlobalChanges}
                                >
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
                        {isEnv && (
                            <div style={{
                                padding: '12px 16px',
                                background: 'rgba(234, 179, 8, 0.1)',
                                border: '1px solid rgba(234, 179, 8, 0.2)',
                                borderRadius: 8,
                                marginBottom: 24,
                                color: '#fbbf24',
                                fontSize: '0.9rem',
                                display: 'flex',
                                alignItems: 'center',
                                gap: 12
                            }}>
                                <SettingsIcon size={18} />
                                <div>
                                    <strong>Managed by Environment</strong>
                                    <div style={{ fontSize: '0.85rem', opacity: 0.9, marginTop: 2 }}>
                                        These settings are set by environment variables (e.g. .env file).
                                    </div>
                                </div>
                            </div>
                        )}
                        <form action="#" onSubmit={(e) => handleSaveSSH(e)} style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 16 }}>
                            <div style={{ gridColumn: 'span 2' }}>
                                <label style={{ display: 'block', marginBottom: 8, fontSize: '0.9em', color: 'var(--text-muted)' }}>Host / IP</label>
                                <input
                                    className="input"
                                    type="text"
                                    disabled={isEnv}
                                    value={sshConfig.host}
                                    onChange={e => setSshConfig({ ...sshConfig, host: e.target.value })}

                                    placeholder="e.g. 192.168.1.100"
                                    style={{ width: '100%', padding: '12px', borderRadius: 8, border: '1px solid rgba(255,255,255,0.1)', background: 'rgba(0,0,0,0.2)', color: 'white', opacity: isEnv ? 0.6 : 1 }}
                                />
                            </div>
                            <div>
                                <label style={{ display: 'block', marginBottom: 8, fontSize: '0.9em', color: 'var(--text-muted)' }}>Port</label>
                                <input
                                    type="number"
                                    disabled={isEnv}
                                    value={sshConfig.port}
                                    onChange={e => setSshConfig({ ...sshConfig, port: parseInt(e.target.value) })}
                                    style={{ width: '100%', padding: '12px', borderRadius: 8, border: '1px solid rgba(255,255,255,0.1)', background: 'rgba(0,0,0,0.2)', color: 'white', opacity: isEnv ? 0.6 : 1 }}
                                />
                            </div>
                            <div>
                                <label style={{ display: 'block', marginBottom: 8, fontSize: '0.9em', color: 'var(--text-muted)' }}>Username</label>
                                <input
                                    type="text"
                                    disabled={isEnv}
                                    value={sshConfig.username}
                                    onChange={e => setSshConfig({ ...sshConfig, username: e.target.value })}
                                    style={{ width: '100%', padding: '12px', borderRadius: 8, border: '1px solid rgba(255,255,255,0.1)', background: 'rgba(0,0,0,0.2)', color: 'white', opacity: isEnv ? 0.6 : 1 }}
                                />

                            </div>

                            {!isEnv && (
                                <div style={{ gridColumn: 'span 2' }}>
                                    <div style={{ display: 'flex', gap: 16, marginBottom: 16, borderBottom: '1px solid rgba(255,255,255,0.1)' }}>
                                        <button
                                            type="button"
                                            onClick={() => setAuthMethod('password')}
                                            style={{
                                                padding: '8px 4px',
                                                background: 'none',
                                                border: 'none',
                                                borderBottom: authMethod === 'password' ? '2px solid var(--primary)' : '2px solid transparent',
                                                color: authMethod === 'password' ? 'var(--text-main)' : 'var(--text-muted)',
                                                cursor: 'pointer',
                                                fontWeight: 500,
                                                display: 'flex',
                                                alignItems: 'center',
                                                gap: 6
                                            }}
                                        >
                                            Password {hasPass && <span style={{ color: 'var(--success)', fontSize: '0.8em' }}>(Stored)</span>}
                                        </button>
                                        <button
                                            type="button"
                                            onClick={() => setAuthMethod('key')}
                                            style={{
                                                padding: '8px 4px',
                                                background: 'none',
                                                border: 'none',
                                                borderBottom: authMethod === 'key' ? '2px solid var(--primary)' : '2px solid transparent',
                                                color: authMethod === 'key' ? 'var(--text-main)' : 'var(--text-muted)',
                                                cursor: 'pointer',
                                                fontWeight: 500,
                                                display: 'flex',
                                                alignItems: 'center',
                                                gap: 6
                                            }}
                                        >
                                            Private Key {hasKey && <span style={{ color: 'var(--success)', fontSize: '0.8em' }}>(Stored)</span>}
                                        </button>
                                    </div>

                                    {authMethod === 'password' && (
                                        <div style={{ marginTop: 8 }}>
                                            <div className="input-group-responsive">
                                                <input
                                                    type="password"
                                                    value={sshConfig.password || ''}
                                                    onChange={e => setSshConfig({ ...sshConfig, password: e.target.value })}
                                                    placeholder={hasPass ? "Leave empty to keep existing" : "Enter password"}
                                                    style={{ flex: 1, padding: '12px', borderRadius: 8, border: '1px solid rgba(255,255,255,0.1)', background: 'rgba(0,0,0,0.2)', color: 'white' }}
                                                />
                                                {(hasPass || sshConfig.password) && (
                                                    <button
                                                        type="button"
                                                        onClick={handleClearPassword}
                                                        className="btn"
                                                        style={{
                                                            display: 'flex',
                                                            alignItems: 'center',
                                                            justifyContent: 'center',
                                                            gap: 6,
                                                            padding: '12px',
                                                            border: '1px solid var(--danger)',
                                                            borderRadius: 8,
                                                            background: 'rgba(220, 38, 38, 0.1)',
                                                            color: 'var(--danger)',
                                                            cursor: 'pointer',
                                                            transition: 'all 0.2s',
                                                            whiteSpace: 'nowrap'
                                                        }}
                                                        title="Clear Password"
                                                    >
                                                        <X size={16} />
                                                        <span style={{ fontSize: '0.85rem', fontWeight: 500 }}>Clear Stored Credential</span>
                                                    </button>
                                                )}
                                            </div>
                                        </div>
                                    )}

                                    {authMethod === 'key' && (
                                        <div style={{ marginTop: 8 }}>
                                            <div className="input-group-responsive">
                                                <input
                                                    type="file"
                                                    onChange={handleKeyUpload}
                                                    style={{
                                                        flex: 1,
                                                        padding: '12px',
                                                        borderRadius: 8,
                                                        border: '1px solid rgba(255,255,255,0.1)',
                                                        background: 'rgba(0,0,0,0.2)',
                                                        color: 'var(--text-muted)'
                                                    }}
                                                />
                                                {sshConfig.privateKey && (
                                                    <span style={{ color: 'var(--success)', fontSize: '0.9rem', display: 'flex', alignItems: 'center', gap: 4 }}>
                                                        <FileText size={16} /> Key Set
                                                    </span>
                                                )}
                                                {(hasKey || sshConfig.privateKey) && (
                                                    <button
                                                        type="button"
                                                        onClick={handleClearKey}
                                                        className="btn"
                                                        style={{
                                                            display: 'flex',
                                                            alignItems: 'center',
                                                            justifyContent: 'center',
                                                            gap: 6,
                                                            padding: '12px',
                                                            border: '1px solid var(--danger)',
                                                            borderRadius: 8,
                                                            background: 'rgba(220, 38, 38, 0.1)',
                                                            color: 'var(--danger)',
                                                            cursor: 'pointer',
                                                            transition: 'all 0.2s',
                                                            whiteSpace: 'nowrap',
                                                            marginLeft: 'auto'
                                                        }}
                                                        title="Clear Private Key"
                                                    >
                                                        <X size={16} />
                                                        <span style={{ fontSize: '0.85rem', fontWeight: 500 }}>Clear Stored Credential</span>
                                                    </button>
                                                )}
                                            </div>
                                            <p style={{ marginTop: 4, fontSize: '0.8rem', color: 'var(--text-muted)' }}>
                                                Upload an OpenSSH private key file (e.g. id_rsa).
                                            </p>
                                        </div>
                                    )}
                                </div>
                            )}



                            <div style={{ gridColumn: 'span 2', marginTop: 16, display: 'flex', alignItems: 'center', gap: 12 }}>
                                <button
                                    type="button"
                                    onClick={handleTestConnection}
                                    className="btn btn-secondary"
                                    style={{ marginRight: 'auto' }}
                                >
                                    <Activity size={18} /> Test Connection
                                </button>
                                {!isEnv && (
                                    <button
                                        type="submit"
                                        className="btn btn-primary"
                                        disabled={!hasSSHChanges}
                                    >
                                        <Save size={18} /> Save SSH Config
                                    </button>
                                )}
                            </div>
                        </form>
                    </div>

                    {/* --- SECTION 3: LOCAL PATHS --- */}
                    <div className="card">
                        <h2 style={{ marginBottom: 24, fontSize: '1.2rem', display: 'flex', alignItems: 'center', gap: 10 }}>
                            <Folder size={20} color="var(--primary)" /> Favorite Paths
                        </h2>
                        <div className="input-group-responsive" style={{ marginBottom: 16 }}>
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
                            <button onClick={() => addLocation()} className="btn btn-primary" disabled={!newLoc || !newLoc.trim()}><Plus size={18} /></button>
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

                    {/* --- SECTION 4: MAINTENANCE --- */}
                    <div className="card" style={{ borderColor: 'rgba(239, 68, 68, 0.3)' }}>
                        <h2 style={{ marginBottom: 24, fontSize: '1.2rem', display: 'flex', alignItems: 'center', gap: 10, color: 'var(--text-main)' }}>
                            <Wrench size={20} color="var(--error)" /> Maintenance
                        </h2>

                        <div style={{ display: 'flex', flexDirection: 'column', gap: 24 }}>
                            {/* Purge Jobs */}
                            <div>
                                <h3 style={{ fontSize: '1.1rem', marginBottom: 4 }}>Purge Job History</h3>
                                <p style={{ color: 'var(--text-muted)', fontSize: '0.9rem', marginBottom: 12 }}>
                                    Remove completed job logs older than the specified number of days.
                                </p>
                                <div style={{ display: 'flex', gap: 12, alignItems: 'center' }}>
                                    <div style={{ flex: 1 }}>
                                        <label style={{ display: 'block', fontSize: '0.8rem', color: 'var(--text-muted)', marginBottom: 4 }}>Days to keep</label>
                                        <input
                                            type="number"
                                            min="0"
                                            value={purgeDays}
                                            onChange={e => setPurgeDays(parseInt(e.target.value) || 0)}
                                            style={{ width: '100%', padding: '10px', borderRadius: 8, border: '1px solid rgba(255,255,255,0.1)', background: 'rgba(0,0,0,0.2)', color: 'white' }}
                                        />
                                    </div>
                                    <button
                                        onClick={handlePurgeHistory}
                                        className="btn btn-primary"
                                        style={{ marginTop: 20 }}
                                    >
                                        Purge
                                    </button>
                                </div>
                            </div>

                            <div style={{ borderTop: '1px solid rgba(255,255,255,0.1)' }}></div>

                            {/* Reset App */}
                            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                                <div>
                                    <h3 style={{ fontSize: '1.1rem', marginBottom: 4, color: 'var(--danger)' }}>Reset Application</h3>
                                    <p style={{ color: 'var(--text-muted)', fontSize: '0.9rem' }}>
                                        DANGER: Wipes all database data, settings, and credentials.
                                    </p>
                                </div>
                                <button
                                    onClick={handleResetApp}
                                    className="btn btn-danger"
                                    style={{ background: 'rgba(220, 38, 38, 0.2)', color: '#fca5a5', border: '1px solid rgba(220, 38, 38, 0.5)' }}
                                >
                                    <AlertTriangle size={16} /> Reset Everything
                                </button>
                            </div>
                        </div>
                    </div>
                </div>
            )
            }

            {/* Local Browser Modal */}
            {
                browserOpen && createPortal(
                    <LocalBrowser
                        currentPath={newLoc || '/app'}
                        onSelect={(path) => {
                            setNewLoc(path);
                            setBrowserOpen(false);
                        }}
                        onClose={() => setBrowserOpen(false)}
                    />,
                    document.body
                )
            }

            <ToastContainer toasts={toasts} removeToast={removeToast} />
        </div >
    );
}
