import { useState, useEffect, useRef } from 'react';
import { createPortal } from 'react-dom';
import { Folder, FileText, ArrowUp, Download, X, ChevronUp, ChevronDown, ChevronRight } from 'lucide-react';
import ModalBackdrop from '../components/ModalBackdrop';
import LocalBrowser from '../components/LocalBrowser';

export default function Browser() {
    const [path, setPath] = useState('/');
    const [files, setFiles] = useState([]);
    const [loading, setLoading] = useState(false);
    const [selectedItem, setSelectedItem] = useState(null);
    const [modalOpen, setModalOpen] = useState(false);
    const [syncLocations, setSyncLocations] = useState([]);
    const [localPath, setLocalPath] = useState('');
    const [isScheduled, setIsScheduled] = useState(true);

    // New state for local browser
    const [showLocalBrowser, setShowLocalBrowser] = useState(false);

    const [error, setError] = useState(null);
    // Sync specific error (inline)
    const [syncError, setSyncError] = useState(null);

    const [folderSizes, setFolderSizes] = useState({});

    // UI state
    const [highlightedPath, setHighlightedPath] = useState(null);
    const [filterQuery, setFilterQuery] = useState('');
    const [sortConfig, setSortConfig] = useState({ key: 'name', direction: 'asc' });

    // Filter AND Sort files - Defined here to be available for useEffect
    const filteredFiles = files
        .filter(file => file.name.toLowerCase().startsWith(filterQuery.toLowerCase()))
        .sort((a, b) => {
            if (a.type !== b.type) {
                return a.type === 'folder' ? -1 : 1;
            }

            let aValue, bValue;

            if (sortConfig.key === 'size') {
                aValue = a.type === 'folder' ? (folderSizes[a.path] || 0) : a.size;
                bValue = b.type === 'folder' ? (folderSizes[b.path] || 0) : b.size;
            } else {
                aValue = a[sortConfig.key];
                bValue = b[sortConfig.key];
            }

            if (typeof aValue === 'string') {
                aValue = aValue.toLowerCase();
                bValue = bValue.toLowerCase();
            }

            if (aValue < bValue) {
                return sortConfig.direction === 'asc' ? -1 : 1;
            }
            if (aValue > bValue) {
                return sortConfig.direction === 'asc' ? 1 : -1;
            }
            return 0;
        });

    const searchInputRef = useRef(null);

    useEffect(() => {
        fetchFiles(path);
        setHighlightedPath(null);
        setFilterQuery('');
    }, [path]);

    useEffect(() => {
        if (modalOpen) {
            fetch('http://localhost:3001/api/sync-locations').then(res => res.json()).then(setSyncLocations);
            setSyncError(null); // Clear previous errors
        }
    }, [modalOpen]);

    // Global Key Listener for Search Auto-Focus and Navigation
    useEffect(() => {
        const handleKeyDown = (e) => {
            // Check if user is already typing in an input or textarea
            const isInput = ['INPUT', 'TEXTAREA'].includes(document.activeElement.tagName) || document.activeElement.isContentEditable;

            // If in an input (other than our search ref, but search ref handles its own via prop), ignore global nav
            if (isInput) return;

            // Navigation: Up/Down/Enter/Backspace
            if (['ArrowDown', 'ArrowUp', 'Home', 'End'].includes(e.key)) {
                e.preventDefault();
                // Ensure focus returns to the list navigation if a button was focused
                if (document.activeElement.tagName === 'BUTTON') {
                    document.activeElement.blur();
                }

                if (filteredFiles.length === 0) return;

                if (e.key === 'Home') {
                    setHighlightedPath(filteredFiles[0].path);
                    return;
                }
                if (e.key === 'End') {
                    setHighlightedPath(filteredFiles[filteredFiles.length - 1].path);
                    return;
                }

                const currentIndex = filteredFiles.findIndex(f => f.path === highlightedPath);
                let newIndex;

                if (e.key === 'ArrowDown') {
                    if (currentIndex === -1) {
                        newIndex = 0;
                    } else {
                        newIndex = currentIndex < filteredFiles.length - 1 ? currentIndex + 1 : currentIndex;
                    }
                } else {
                    // ArrowUp
                    if (currentIndex === -1) {
                        newIndex = filteredFiles.length - 1;
                    } else {
                        newIndex = currentIndex > 0 ? currentIndex - 1 : 0;
                    }
                }
                setHighlightedPath(filteredFiles[newIndex].path);
                return;
            }

            if (e.key === 'Enter') {
                if (document.activeElement.tagName === 'BUTTON') return;
                const currentIndex = filteredFiles.findIndex(f => f.path === highlightedPath);
                // If nothing highlighted, maybe enter first item? Or do nothing? 
                // Let's match expected behavior: if highlighted, act.
                if (currentIndex !== -1) {
                    e.preventDefault();
                    const file = filteredFiles[currentIndex];
                    if (file.type === 'folder') {
                        handleNavigate(file.path);
                    }
                    // Files: do nothing (Tab requirement)
                }
                return;
            }

            if (e.key === 'Backspace') {
                e.preventDefault();
                handleUp();
                return;
            }

            // Auto-Focus: Ignore special keys (ctrl, alt, meta) and non-character keys
            if (e.ctrlKey || e.altKey || e.metaKey || e.key.length > 1) {
                return;
            }

            // If it's a valid character, focus the search input
            if (/^[a-zA-Z0-9]$/.test(e.key)) {
                if (searchInputRef.current) {
                    searchInputRef.current.focus();
                }
            }
        };

        window.addEventListener('keydown', handleKeyDown);
        return () => window.removeEventListener('keydown', handleKeyDown);
    }, [filteredFiles, highlightedPath, path]); // Dependencies for closure

    const fetchFiles = async (p) => {
        setLoading(true);
        setFiles([]); // Clear previous files
        setFolderSizes({}); // Clear previous sizes
        setError(null);
        try {
            const res = await fetch(`http://localhost:3001/api/remote/list?path=${encodeURIComponent(p)}`);
            const data = await res.json();

            if (res.ok) {
                setFiles(data);
                // Lazily fetch folder sizes
                const folders = data.filter(f => f.type === 'folder').map(f => f.path);
                if (folders.length > 0) {
                    fetchSizes(folders);
                }
            } else {
                // Handle different error types
                if (data.error === 'Missing SSH configuration') {
                    setError({
                        title: 'Configuration Required',
                        message: 'SSH Configuration is missing. Please configure your server details to continue.',
                        action: { label: 'Go to Settings', link: '/settings' }
                    });
                } else if (data.error.includes('Authentication failure')) {
                    setError({
                        title: 'Authentication Failed',
                        message: 'Please check your username, password, or SSH key in Settings.',
                        action: { label: 'Go to Settings', link: '/settings' }
                    });
                } else if (data.error.includes('ENOTFOUND') || data.error.includes('ETIMEDOUT')) {
                    setError({
                        title: 'Connection Failed',
                        message: 'Could not connect to server. Please check the Host/IP and Port.'
                    });
                } else {
                    setError({
                        title: 'Error',
                        message: data.error
                    });
                }
            }
        } catch (err) {
            console.error(err);
            setError({
                title: 'Network Error',
                message: 'Could not connect to the backend server. Please ensure the backend is running.'
            });
        } finally {
            setLoading(false);
        }
    };

    const fetchSizes = async (paths) => {
        try {
            const res = await fetch('http://localhost:3001/api/remote/folders/sizes', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ paths })
            });
            if (res.ok) {
                const sizes = await res.json();
                setFolderSizes(prev => ({ ...prev, ...sizes }));
            }
        } catch (err) {
            console.error('Error fetching sizes:', err);
        }
    };

    const handleNavigate = (newPath) => {
        setPath(newPath);
    };

    const handleUp = () => {
        if (path === '/') return;
        const parent = path.substring(0, path.lastIndexOf('/')) || '/';
        setPath(parent);
    };

    const openSyncModal = (item) => {
        setSelectedItem(item);
        setLocalPath('');
        setIsScheduled(true);
        setModalOpen(true);
    };

    const handleSync = async () => {
        if (!localPath) return;

        setSyncError(null);

        let finalLocalPath = localPath;
        // Simple heuristic: if using a preset, append name
        const isPreset = syncLocations.find(l => l.path === localPath);
        if (isPreset) {
            finalLocalPath = `${localPath}/${selectedItem.name}`;
        }

        try {
            const res = await fetch('http://localhost:3001/api/sync', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    remotePath: selectedItem.path,
                    localPath: finalLocalPath,
                    type: selectedItem.type,
                    active: isScheduled
                })
            });
            const data = await res.json();
            if (!res.ok) throw new Error(data.error);

            setModalOpen(false);
            // Optional: show a toast or success check
        } catch (err) {
            setSyncError(err.message);
        }
    };

    const formatBytes = (bytes, decimals = 1) => {
        if (!bytes) return '-';
        if (bytes === 0) return '0 B';

        const k = 1024;
        const dm = decimals < 0 ? 0 : decimals;
        const sizes = ['B', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB'];

        const i = Math.floor(Math.log(bytes) / Math.log(k));

        return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + ' ' + sizes[i];
    };

    const formatDate = (ts) => {
        if (!ts) return '-';
        return new Date(ts).toLocaleString();
    };

    const handleSort = (key) => {
        let direction = 'asc';
        if (sortConfig.key === key && sortConfig.direction === 'asc') {
            direction = 'desc';
        }
        setSortConfig({ key, direction });
    };



    const SortIcon = ({ columnKey }) => {
        if (sortConfig.key !== columnKey) return <div style={{ width: 16 }} />; // spacer
        return sortConfig.direction === 'asc' ? <ChevronUp size={14} /> : <ChevronDown size={14} />;
    };

    const handleInputKeyDown = (e) => {
        if (e.key === 'Escape') {
            e.currentTarget.blur();
            return;
        }

        if (filteredFiles.length === 0) return;

        const currentIndex = filteredFiles.findIndex(f => f.path === highlightedPath);

        if (e.key === 'ArrowDown' || e.key === 'ArrowUp') {
            e.preventDefault();
            e.currentTarget.blur();
        } else if (e.key === 'Backspace' && filterQuery === '') {
            e.preventDefault();
            handleUp();
        } else if (e.key === 'Tab') {
            e.preventDefault();
            const targetIndex = currentIndex !== -1 ? currentIndex : 0;
            const btn = document.getElementById(`sync-btn-${targetIndex}`);
            if (btn) btn.focus();
        } else if (e.key === 'Enter') {
            e.preventDefault();
            const file = currentIndex !== -1 ? filteredFiles[currentIndex] : filteredFiles[0];
            if (file) {
                if (file.type === 'folder') {
                    handleNavigate(file.path);
                }
                // For files, do nothing. User must Tab to sync.
            }
        }
    };

    useEffect(() => {
        if (highlightedPath) {
            const index = filteredFiles.findIndex(f => f.path === highlightedPath);
            if (index !== -1) {
                const el = document.getElementById(`file-row-${index}`);
                if (el) {
                    el.scrollIntoView({ block: 'nearest' });
                }
            }
        }
    }, [highlightedPath, filteredFiles]);

    const headerStyle = { cursor: 'pointer', userSelect: 'none', display: 'flex', alignItems: 'center', gap: 6 };

    return (
        <div className="animate-enter">
            {/* ... header ... */}
            <div className="header">
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
                    <div style={{ width: '100%' }}>
                        <h1 className="page-title">Remote Browser</h1>
                        <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginTop: 8, color: 'var(--text-muted)', width: '100%' }}>
                            <button
                                onClick={handleUp}
                                disabled={path === '/'}
                                style={{
                                    opacity: path === '/' ? 0.3 : 1,
                                    border: '1px solid rgba(255,255,255,0.2)',
                                    background: 'rgba(255,255,255,0.05)',
                                    borderRadius: 6,
                                    padding: 6,
                                    cursor: path === '/' ? 'default' : 'pointer',
                                    display: 'flex',
                                    alignItems: 'center',
                                    justifyContent: 'center'
                                }}
                            >
                                <ArrowUp size={16} color="white" />
                            </button>
                            <div className="breadcrumbs-desktop">
                                <button
                                    onClick={() => handleNavigate('/')}
                                    style={{
                                        background: path === '/' ? 'var(--primary)' : 'rgba(255,255,255,0.1)',
                                        padding: '4px 12px',
                                        borderRadius: 16,
                                        fontSize: '0.85em',
                                        color: 'white',
                                        border: 'none',
                                        cursor: 'pointer',
                                        transition: 'all 0.2s'
                                    }}
                                    onMouseEnter={e => path !== '/' && (e.target.style.background = 'rgba(255,255,255,0.2)')}
                                    onMouseLeave={e => path !== '/' && (e.target.style.background = 'rgba(255,255,255,0.1)')}
                                >
                                    root
                                </button>
                                {path.split('/').filter(Boolean).map((segment, index, arr) => {
                                    const segmentPath = '/' + arr.slice(0, index + 1).join('/');
                                    const isLast = index === arr.length - 1;
                                    return (
                                        <button
                                            key={segmentPath}
                                            onClick={() => handleNavigate(segmentPath)}
                                            disabled={isLast}
                                            style={{
                                                background: isLast ? 'var(--primary)' : 'rgba(255,255,255,0.1)',
                                                padding: '4px 12px',
                                                borderRadius: 16,
                                                fontSize: '0.85em',
                                                color: 'white',
                                                border: 'none',
                                                cursor: isLast ? 'default' : 'pointer',
                                                transition: 'all 0.2s',
                                                maxWidth: 200,
                                                overflow: 'hidden',
                                                textOverflow: 'ellipsis',
                                                whiteSpace: 'nowrap'
                                            }}
                                            onMouseEnter={e => !isLast && (e.target.style.background = 'rgba(255,255,255,0.2)')}
                                            onMouseLeave={e => !isLast && (e.target.style.background = 'rgba(255,255,255,0.1)')}
                                        >
                                            {segment}
                                        </button>
                                    );
                                })}
                            </div>

                            <div className="breadcrumbs-mobile" style={{ flex: 1 }}>
                                <div style={{ position: 'relative', width: '100%', display: 'flex' }}>
                                    <select
                                        value={path}
                                        onChange={(e) => handleNavigate(e.target.value)}
                                        style={{
                                            appearance: 'none',
                                            width: '100%',
                                            background: 'rgba(255,255,255,0.05)',
                                            border: '1px solid rgba(255,255,255,0.2)',
                                            borderRadius: 6,
                                            padding: '8px 30px 8px 12px',
                                            color: 'white',
                                            fontSize: '0.9em',
                                            outline: 'none',
                                            cursor: 'pointer'
                                        }}
                                    >
                                        <option value="/">root</option>
                                        {path.split('/').filter(Boolean).map((segment, index, arr) => {
                                            const segmentPath = '/' + arr.slice(0, index + 1).join('/');
                                            return (
                                                <option key={segmentPath} value={segmentPath}>
                                                    {segmentPath}
                                                </option>
                                            );
                                        })}
                                    </select>
                                    <ChevronDown size={14} style={{ position: 'absolute', right: 10, top: '50%', transform: 'translateY(-50%)', pointerEvents: 'none', color: 'var(--text-muted)' }} />
                                </div>
                            </div>
                        </div>
                    </div>


                </div>
            </div>

            <div className="card">
                <div style={{ position: 'relative', width: '100%', marginBottom: 16 }}>
                    <input
                        ref={searchInputRef}
                        type="text"
                        placeholder="Filter files..."
                        value={filterQuery}
                        onChange={e => setFilterQuery(e.target.value)}
                        onKeyDown={handleInputKeyDown}
                        style={{
                            width: '100%',
                            padding: '10px 12px',
                            paddingRight: 32,
                            borderRadius: 6,
                            border: '1px solid rgba(255,255,255,0.1)',
                            background: 'rgba(0,0,0,0.2)',
                            color: 'white',
                            fontSize: '0.9em'
                        }}
                    />
                    {filterQuery && (
                        <button
                            onClick={() => setFilterQuery('')}
                            style={{
                                position: 'absolute',
                                right: 10,
                                top: '50%',
                                transform: 'translateY(-50%)',
                                background: 'none',
                                border: 'none',
                                color: 'var(--text-muted)',
                                cursor: 'pointer',
                                display: 'flex'
                            }}
                        >
                            <X size={14} />
                        </button>
                    )}
                </div>

                {loading ? <p>Loading remote files...</p> : (
                    <div
                        className="table-wrap"
                        onClick={() => setHighlightedPath(null)}
                        style={{ minHeight: 300 }} // Ensure clickable area
                    >
                        <table style={{ borderCollapse: 'collapse', width: '100%' }}>
                            <thead>
                                <tr>
                                    <th onClick={() => handleSort('name')}>
                                        <div style={headerStyle}>Name <SortIcon columnKey="name" /></div>
                                    </th>
                                    <th onClick={() => handleSort('size')}>
                                        <div style={headerStyle}>Size <SortIcon columnKey="size" /></div>
                                    </th>
                                    <th onClick={() => handleSort('mtime')}>
                                        <div style={headerStyle}>Date Modified <SortIcon columnKey="mtime" /></div>
                                    </th>
                                    <th onClick={() => handleSort('atime')}>
                                        <div style={headerStyle}>Last Accessed <SortIcon columnKey="atime" /></div>
                                    </th>
                                    <th style={{ textAlign: 'right' }}>Action</th>
                                </tr>
                            </thead>
                            <tbody>
                                {filteredFiles.length === 0 && !loading && (
                                    <tr>
                                        <td colSpan={6} style={{ padding: 20, textAlign: 'center', color: 'var(--text-muted)' }}>
                                            {filterQuery ? 'No matching files found' : 'Empty directory'}
                                        </td>
                                    </tr>
                                )}
                                {filteredFiles.map((file, index) => {
                                    const isSelected = highlightedPath === file.path;
                                    return (
                                        <tr
                                            key={file.name}
                                            id={`file-row-${index}`}
                                            onClick={(e) => {
                                                e.stopPropagation();
                                                if (isSelected && file.type === 'folder') {
                                                    handleNavigate(file.path);
                                                } else {
                                                    setHighlightedPath(file.path);
                                                }
                                            }}
                                            onDoubleClick={() => {
                                                if (file.type === 'folder') {
                                                    handleNavigate(file.path);
                                                }
                                            }}
                                            style={{
                                                cursor: 'pointer',
                                                background: isSelected ? 'rgba(50, 150, 255, 0.1)' : 'transparent',
                                                borderBottom: '1px solid rgba(255,255,255,0.05)',
                                                transition: 'background 0.1s'
                                            }}
                                            className="file-row"
                                        >
                                            <td>
                                                <div style={{ display: 'flex', alignItems: 'center', width: '100%' }}>
                                                    <div style={{ marginRight: 12, display: 'flex', alignItems: 'center' }}>
                                                        {file.type === 'folder' ? <Folder color={isSelected ? 'var(--primary)' : "var(--primary)"} size={20} /> : <FileText color="var(--text-muted)" size={20} />}
                                                    </div>
                                                    <span style={{
                                                        fontWeight: file.type === 'folder' ? 500 : 400,
                                                        color: isSelected ? 'white' : 'var(--text-main)',
                                                        flex: 1,
                                                        overflow: 'hidden',
                                                        textOverflow: 'ellipsis',
                                                        whiteSpace: 'nowrap',
                                                        marginRight: 8
                                                    }}>
                                                        {file.name}
                                                    </span>
                                                    {file.type === 'folder' && (
                                                        <button
                                                            className="btn-icon"
                                                            onClick={(e) => {
                                                                e.stopPropagation();
                                                                handleNavigate(file.path);
                                                            }}
                                                            style={{
                                                                background: 'none',
                                                                border: 'none',
                                                                padding: 4,
                                                                cursor: 'pointer',
                                                                color: isSelected ? 'white' : 'var(--text-muted)',
                                                                display: 'flex',
                                                                alignItems: 'center',
                                                                flexShrink: 0
                                                            }}
                                                            title="Open Folder"
                                                        >
                                                            <ChevronRight size={16} />
                                                        </button>
                                                    )}
                                                </div>
                                            </td>
                                            <td data-label="Size" style={{ fontSize: '0.9em', color: 'var(--text-muted)' }}>
                                                {
                                                    file.type === 'folder'
                                                        ? (folderSizes[file.path] !== undefined ? formatBytes(folderSizes[file.path]) : '-')
                                                        : formatBytes(file.size)
                                                }
                                            </td>
                                            <td data-label="Date Modified" style={{ fontSize: '0.9em', color: 'var(--text-muted)' }}>
                                                {formatDate(file.mtime)}
                                            </td>
                                            <td data-label="Last Accessed" style={{ fontSize: '0.9em', color: 'var(--text-muted)' }}>
                                                {formatDate(file.atime)}
                                            </td>
                                            <td style={{ textAlign: 'right' }}>
                                                <button
                                                    id={`sync-btn-${index}`}
                                                    onClick={(e) => {
                                                        e.stopPropagation();
                                                        openSyncModal(file);
                                                    }}
                                                    onKeyDown={(e) => {
                                                        if (e.key === 'Tab') {
                                                            e.preventDefault();
                                                            searchInputRef.current?.focus();
                                                        }
                                                    }}
                                                    className="btn btn-secondary"
                                                    style={{ padding: '6px 12px', fontSize: '0.8rem' }}
                                                >
                                                    <Download size={14} /> Sync
                                                </button>
                                            </td>
                                        </tr>
                                    );
                                })}
                            </tbody>
                        </table>
                    </div>
                )}
            </div>

            {/* Error Modal (Global) */}
            {error && createPortal(
                <ModalBackdrop onClose={() => setError(null)}>
                    <div className="card" style={{ width: 400, maxWidth: '90%', border: '1px solid var(--error)' }}>
                        <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 16 }}>
                            <h3 style={{ color: 'var(--error)' }}>{error.title}</h3>
                            <button onClick={() => setError(null)}><X size={20} color="white" /></button>
                        </div>
                        <p style={{ marginBottom: 24, lineHeight: 1.6 }}>{error.message}</p>
                        <div style={{ display: 'flex', justifyContent: 'flex-end', gap: 12 }}>
                            <button onClick={() => setError(null)} className="btn btn-secondary">Close</button>
                            {error.action && (
                                <button onClick={() => window.location.href = error.action.link} className="btn btn-primary">
                                    {error.action.label}
                                </button>
                            )}
                        </div>
                    </div>
                </ModalBackdrop>,
                document.body
            )}

            {/* Sync Modal */}
            {modalOpen && createPortal(
                <ModalBackdrop onClose={() => setModalOpen(false)}>
                    <div className="card" style={{ width: 500, maxWidth: '90%' }}>
                        <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 20 }}>
                            <h3>Sync "{selectedItem?.name}"</h3>
                            <button onClick={() => setModalOpen(false)}><X size={20} color="white" /></button>
                        </div>

                        <div style={{ marginBottom: 20 }}>
                            <label style={{ display: 'block', marginBottom: 8 }}>Select Local Destination:</label>

                            <div style={{ display: 'flex', flexWrap: 'wrap', gap: 8, marginBottom: 16 }}>
                                {syncLocations.map(loc => (
                                    <button
                                        key={loc.id}
                                        onClick={() => setLocalPath(loc.path)}
                                        style={{
                                            padding: '8px 12px', borderRadius: 6,
                                            background: localPath === loc.path ? 'var(--primary)' : 'rgba(255,255,255,0.1)',
                                            color: 'white', border: 'none'
                                        }}
                                    >
                                        {loc.path}
                                    </button>
                                ))}
                            </div>

                            <div style={{ display: 'flex', gap: 8 }}>
                                <input
                                    type="text"
                                    value={localPath}
                                    onChange={e => setLocalPath(e.target.value)}
                                    placeholder="/absolute/path/to/local/folder"
                                    style={{ flex: 1, padding: '12px', borderRadius: 8, border: '1px solid rgba(255,255,255,0.1)', background: 'rgba(0,0,0,0.2)', color: 'white' }}
                                />
                                <button
                                    className="btn btn-secondary"
                                    onClick={() => setShowLocalBrowser(true)}
                                    title="Browse Local Storage"
                                    style={{ display: 'flex', alignItems: 'center', gap: 6, whiteSpace: 'nowrap' }}
                                >
                                    <Folder size={18} /> Browse
                                </button>
                            </div>
                            <p style={{ fontSize: '0.8rem', color: 'var(--text-muted)', marginTop: 8 }}>
                                Note: If you selected a preset location, the {selectedItem?.type || 'item'} "{selectedItem?.name}" will be created inside it.
                            </p>
                        </div>

                        {syncError && (
                            <div style={{ padding: '10px 14px', background: 'rgba(220, 38, 38, 0.1)', border: '1px solid var(--error)', borderRadius: 8, color: 'var(--error)', marginBottom: 20, fontSize: '0.9em' }}>
                                ⚠ {syncError}
                            </div>
                        )}

                        <div style={{ marginBottom: 24, padding: '12px', background: 'rgba(255,255,255,0.05)', borderRadius: 8 }}>
                            <label style={{ display: 'flex', alignItems: 'center', cursor: 'pointer', gap: 12 }}>
                                <input
                                    type="checkbox"
                                    checked={isScheduled}
                                    onChange={e => setIsScheduled(e.target.checked)}
                                    style={{ width: 18, height: 18, accentColor: 'var(--primary)' }}
                                />
                                <div>
                                    <span style={{ display: 'block', fontWeight: 500 }}>Enable Auto-Sync</span>
                                    <span style={{ display: 'block', fontSize: '0.85rem', color: 'var(--text-muted)' }}>
                                        {isScheduled
                                            ? "Item will be checked for updates according to the global schedule."
                                            : "This will be a one-time sync. You can enable auto-sync later."}
                                    </span>
                                </div>
                            </label>
                        </div>

                        <div style={{ display: 'flex', justifyContent: 'flex-end', gap: 12 }}>
                            <button onClick={() => setModalOpen(false)} className="btn btn-secondary">Cancel</button>
                            <button
                                onClick={handleSync}
                                disabled={!localPath}
                                className="btn btn-primary"
                                style={{ opacity: !localPath ? 0.5 : 1, cursor: !localPath ? 'not-allowed' : 'pointer' }}
                            >
                                Start Sync
                            </button>
                        </div>
                    </div>
                </ModalBackdrop>,
                document.body
            )}

            {/* Local Browser Modal */}
            {showLocalBrowser && createPortal(
                <LocalBrowser
                    onClose={() => setShowLocalBrowser(false)}
                    onSelect={(selectedPath) => setLocalPath(selectedPath)}
                    currentPath={localPath || '/app'}
                />,
                document.body
            )}
        </div>
    );
}


