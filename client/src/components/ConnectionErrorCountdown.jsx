import React, { useState, useEffect } from 'react';

// Helper to format absolute time (HH:MM:SS)
const formatTime = (isoString) => {
    try {
        return new Date(isoString).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
    } catch (e) {
        return isoString;
    }
};

// Helper component for live countdowns
const ConnectionErrorCountdown = ({ log }) => {
    const [now, setNow] = useState(Date.now());

    useEffect(() => {
        const timer = setInterval(() => setNow(Date.now()), 1000);
        return () => clearInterval(timer);
    }, []);

    // Parse specific format: "Connection lost: <Error>. Next retry at <ISO>. Timeout at <ISO>."
    // Regex to extract ISO timestamps
    const retryMatch = log.match(/Next retry at ([\d\-\:T\.]+Z)/);
    const timeoutMatch = log.match(/Timeout at ([\d\-\:T\.]+Z)/);

    const retryTargetISO = retryMatch ? retryMatch[1] : null;
    const timeoutTargetISO = timeoutMatch ? timeoutMatch[1] : null;

    // Extract base error message (everything before "Next retry")
    const baseError = log.split('Next retry')[0].replace('Connection lost:', '').trim();

    if (!retryTargetISO || !timeoutTargetISO) {
        // Fallback if parsing fails
        return (
            <div style={{ fontSize: '0.8em', color: 'var(--warning)', marginTop: 4, fontWeight: 500 }}>
                ⚠ {log}
            </div>
        );
    }

    const retryDate = new Date(retryTargetISO);
    const timeoutDate = new Date(timeoutTargetISO);

    const retryDiff = Math.max(0, Math.ceil((retryDate - now) / 1000));
    const timeoutDiff = Math.max(0, Math.ceil((timeoutDate - now) / 1000));

    // Format remaining time as Xm Ys
    const formatDiff = (seconds) => {
        if (seconds <= 0) return '0s';
        const m = Math.floor(seconds / 60);
        const s = seconds % 60;
        return m > 0 ? `${m}m ${s}s` : `${s}s`;
    };

    return (
        <div style={{ fontSize: '0.8em', color: 'var(--warning)', marginTop: 4, fontWeight: 500 }}>
            <div style={{ marginBottom: 2 }}>⚠ Connection lost: {baseError}</div>
            <div style={{ display: 'flex', gap: '12px', fontSize: '0.95em', opacity: 0.9 }}>
                <span>
                    Retry: <strong>{formatTime(retryTargetISO)}</strong> ({formatDiff(retryDiff)})
                </span>
                <span>
                    Timeout: <strong>{formatTime(timeoutTargetISO)}</strong> ({formatDiff(timeoutDiff)})
                </span>
            </div>
        </div>
    );
};

export default ConnectionErrorCountdown;
