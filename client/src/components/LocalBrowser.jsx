import { useState, useEffect } from 'react';
import { Folder, FileText, ArrowUp, Check, X, Loader, ChevronRight } from 'lucide-react';
import ModalBackdrop from './ModalBackdrop';

export default function LocalBrowser({ onClose, onSelect, currentPath = '/' }) {
    const [path, setPath] = useState(currentPath === '' ? '/' : currentPath);
    const [files, setFiles] = useState([]);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState(null);

    const [selectedFolder, setSelectedFolder] = useState(null);

    useEffect(() => {
        fetchFiles(path);
        setSelectedFolder(null); // Deselect on navigation
    }, [path]);

    const fetchFiles = async (p) => {
        setLoading(true);
        setError(null);
        try {
            // If p is empty, default to /
            let target = p || '/';
            // Ensure target doesn't end in slash unless it is root, for cleaner API calls
            if (target !== '/' && target.endsWith('/')) {
                target = target.slice(0, -1);
            }

            const res = await fetch(`/api/local/list?path=${encodeURIComponent(target)}`);
            const data = await res.json();

            if (res.ok) {
                setFiles(data);
            } else {
                setError(data.error);
            }
        } catch (err) {
            setError(err.message);
        } finally {
            setLoading(false);
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

    const handleSelectCurrent = () => {
        // If a subfolder is highlighted, return that. Otherwise return current path.
        if (selectedFolder) {
            onSelect(selectedFolder);
        } else {
            onSelect(path);
        }
        onClose();
    };

    const formatBytes = (bytes) => {
        if (!bytes) return '-';
        if (bytes === 0) return '0 B';
        const k = 1024;
        const i = Math.floor(Math.log(bytes) / Math.log(k));
        const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
        return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i];
    };

    // Derived path for display (if selected, show that)
    const displayPath = selectedFolder || path;

    return (
        <ModalBackdrop onClose={onClose}>
            <div className="card" style={{ width: 600, maxWidth: '95%', height: '80vh', display: 'flex', flexDirection: 'column' }}>
                {/* Header */}
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 16, borderBottom: '1px solid rgba(255,255,255,0.1)', paddingBottom: 16 }}>
                    <h3 style={{ margin: 0 }}>Browse Local Storage</h3>
                    <button onClick={onClose}><X size={20} color="white" /></button>
                </div>

                {/* Path Bar */}
                <div style={{ display: 'flex', gap: 8, marginBottom: 12 }}>
                    <button
                        onClick={handleUp}
                        disabled={path === '/'}
                        className="btn btn-secondary"
                        style={{ padding: '6px 10px' }}
                    >
                        <ArrowUp size={16} />
                    </button>
                    <div style={{
                        flex: 1,
                        background: 'rgba(0,0,0,0.2)',
                        padding: '6px 12px',
                        borderRadius: 6,
                        border: '1px solid rgba(255,255,255,0.1)',
                        display: 'flex',
                        alignItems: 'center',
                        fontFamily: 'monospace',
                        fontSize: '0.9em'
                    }}>
                        {displayPath}
                    </div>
                </div>

                {/* Content */}
                <div
                    style={{ flex: 1, overflowY: 'auto', border: '1px solid rgba(255,255,255,0.1)', borderRadius: 6, background: 'rgba(0,0,0,0.1)' }}
                    onClick={() => setSelectedFolder(null)}
                >
                    {loading ? (
                        <div style={{ display: 'flex', justifyContent: 'center', padding: 40 }}>
                            <Loader className="spin" />
                        </div>
                    ) : error ? (
                        <div style={{ padding: 20, color: 'var(--error)' }}>
                            Error: {error}
                        </div>
                    ) : (
                        <table style={{ width: '100%', borderCollapse: 'collapse' }}>
                            <thead style={{ position: 'sticky', top: 0, background: '#1a1a1a', zIndex: 1 }}>
                                <tr style={{ borderBottom: '1px solid rgba(255,255,255,0.1)' }}>
                                    <th style={{ padding: 8, textAlign: 'left' }}>Name</th>
                                </tr>
                            </thead>
                            <tbody>
                                {files.filter(f => f.type === 'folder').map(file => {
                                    const isSelected = selectedFolder === file.path;
                                    return (
                                        <tr
                                            key={file.name}
                                            className="hover-row"
                                            style={{
                                                cursor: 'pointer',
                                                borderBottom: '1px solid rgba(255,255,255,0.05)',
                                                background: isSelected ? 'rgba(50, 150, 255, 0.1)' : 'transparent' // Highlight selected (Blue)
                                            }}
                                            onClick={(e) => {
                                                e.stopPropagation();
                                                if (isSelected) {
                                                    handleNavigate(file.path);
                                                } else {
                                                    setSelectedFolder(file.path);
                                                }
                                            }}
                                            onDoubleClick={() => handleNavigate(file.path)}
                                        >
                                            <td style={{ padding: 8, fontWeight: isSelected ? 600 : 400 }}>
                                                <div style={{ display: 'flex', alignItems: 'center', width: '100%' }}>
                                                    <div style={{ marginRight: 12, display: 'flex', alignItems: 'center' }}>
                                                        <Folder color={isSelected ? 'var(--primary)' : "var(--text-muted)"} size={18} />
                                                    </div>
                                                    <span style={{ flex: 1, whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis', marginRight: 8 }}>{file.name}</span>
                                                    <button
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
                                                </div>
                                            </td>
                                        </tr>
                                    );
                                })}
                                {files.length === 0 && (
                                    <tr>
                                        <td colSpan={2} style={{ padding: 20, textAlign: 'center', color: 'var(--text-muted)' }}>Empty directory</td>
                                    </tr>
                                )}
                            </tbody>
                        </table>
                    )}
                </div>

                {/* Footer */}
                <div style={{ marginTop: 16, display: 'flex', justifyContent: 'flex-end', gap: 12, alignItems: 'center' }}>
                    <div style={{ marginRight: 'auto', fontSize: '0.85em', color: 'var(--text-muted)' }}>
                        <InfoIcon /> Double-click to open. Single-click to select.
                    </div>
                    <button onClick={onClose} className="btn btn-secondary">Cancel</button>
                    <button onClick={handleSelectCurrent} className="btn btn-primary" style={{ display: 'flex', gap: 8, alignItems: 'center', whiteSpace: 'nowrap' }}>
                        <Check size={16} /> Select This Folder
                    </button>
                </div>
            </div>
        </ModalBackdrop>
    );
}

function InfoIcon() {
    return (
        <span style={{ display: 'inline-flex', justifyContent: 'center', alignItems: 'center', width: 16, height: 16, borderRadius: '50%', border: '1px solid currentColor', marginRight: 6, fontSize: 10 }}>i</span>
    )
}
