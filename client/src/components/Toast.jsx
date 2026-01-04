
import { useEffect, useState } from 'react';
import { createPortal } from 'react-dom';
import { X, CheckCircle, AlertTriangle, AlertCircle } from 'lucide-react';

export default function ToastContainer({ toasts, removeToast }) {
    if (!toasts || toasts.length === 0) return null;

    return createPortal(
        <div className="toast-container">
            {toasts.map(toast => (
                <Toast key={toast.id} toast={toast} onClose={() => removeToast(toast.id)} />
            ))}
        </div>,
        document.body
    );
}

function Toast({ toast, onClose }) {
    const [isHovered, setIsHovered] = useState(false);

    // Auto-dismiss logic
    useEffect(() => {
        if (!toast.duration || isHovered) return;

        const timer = setTimeout(() => {
            onClose();
        }, toast.duration);

        return () => clearTimeout(timer);
    }, [toast.duration, isHovered, onClose]);

    const getStyles = () => {
        switch (toast.type) {
            case 'success': return { bg: 'var(--success)', icon: <CheckCircle size={18} color="white" /> };
            case 'error': return { bg: 'var(--error)', icon: <AlertCircle size={18} color="white" /> };
            case 'warning': return { bg: 'var(--warning)', icon: <AlertTriangle size={18} color="white" /> };
            default: return { bg: 'var(--card-bg)', icon: null };
        }
    };

    const styles = getStyles();

    return (
        <div
            className="animate-enter-up"
            onMouseEnter={() => setIsHovered(true)}
            onMouseLeave={() => setIsHovered(false)}
            style={{
                pointerEvents: 'auto',
                background: styles.bg,
                color: 'white',
                padding: '12px 16px',
                borderRadius: 'var(--radius-md)',
                display: 'flex',
                alignItems: 'center',
                gap: 12,
                boxShadow: '0 4px 12px rgba(0,0,0,0.3)',
                minWidth: 300,
                maxWidth: '90%'
            }}
        >
            {styles.icon}
            <span style={{ flex: 1, fontSize: '0.95rem' }}>{toast.message}</span>
            <button
                onClick={onClose}
                style={{
                    background: 'none',
                    border: 'none',
                    color: 'rgba(255,255,255,0.8)',
                    cursor: 'pointer',
                    display: 'flex',
                    padding: 4
                }}
            >
                <X size={16} />
            </button>
        </div>
    );
}
