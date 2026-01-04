import { useState, useEffect, useRef } from 'react';
import { RefreshCw } from 'lucide-react';

export default function PullToRefresh({ onRefresh, children }) {
    const [startPoint, setStartPoint] = useState(0);
    const [pullChange, setPullChange] = useState(0);
    const [refreshing, setRefreshing] = useState(false);
    const contentRef = useRef(null);

    useEffect(() => {
        const element = contentRef.current;
        if (!element) return;

        const handleTouchStart = (e) => {
            const scrollTop = window.scrollY || document.documentElement.scrollTop;
            if (scrollTop === 0) {
                setStartPoint(e.targetTouches[0].screenY);
            }
        };

        const handleTouchMove = (e) => {
            if (!startPoint) return;
            const scrollTop = window.scrollY || document.documentElement.scrollTop;
            const currentPoint = e.targetTouches[0].screenY;
            const change = currentPoint - startPoint;

            if (scrollTop === 0 && change > 0) {
                // Prevent default pull-to-refresh behavior in some browsers if we can,
                // but usually passive listeners prevent e.preventDefault().
                // We'll just rely on the visual feedback.
                setPullChange(change > 150 ? 150 : change); // Cap at 150px
            }
        };

        const handleTouchEnd = async () => {
            if (!startPoint) return;

            if (pullChange > 80) { // Threshold to trigger refresh
                setRefreshing(true);
                setPullChange(60); // Snap to loading position
                try {
                    await onRefresh();
                } finally {
                    setTimeout(() => {
                        setRefreshing(false);
                        setPullChange(0);
                    }, 500);
                }
            } else {
                setPullChange(0);
            }
            setStartPoint(0);
        };

        element.addEventListener('touchstart', handleTouchStart, { passive: true });
        element.addEventListener('touchmove', handleTouchMove, { passive: true });
        element.addEventListener('touchend', handleTouchEnd, { passive: true });

        return () => {
            element.removeEventListener('touchstart', handleTouchStart);
            element.removeEventListener('touchmove', handleTouchMove);
            element.removeEventListener('touchend', handleTouchEnd);
        };
    }, [startPoint, pullChange, onRefresh]);

    return (
        <div
            ref={contentRef}
            style={{
                minHeight: '100vh', /* Ensure it fills screen so it catches touches */
                position: 'relative'
            }}
        >
            <div style={{
                position: 'absolute',
                top: -50,
                left: 0,
                width: '100%',
                height: 50,
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                transform: `translateY(${pullChange > 0 ? pullChange + 50 : 0}px)`, /* Move down with pull */
                transition: refreshing ? 'transform 0.2s' : 'none', /* Snap back animation */
                opacity: pullChange > 0 ? 1 : 0,
                zIndex: 100
            }}>
                <div style={{
                    background: 'var(--bg-card)',
                    borderRadius: '50%',
                    width: 40,
                    height: 40,
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'center',
                    boxShadow: '0 4px 12px rgba(0,0,0,0.2)',
                    transform: `rotate(${pullChange * 2}deg)`
                }}>
                    <RefreshCw size={20} className={refreshing ? 'spin' : ''} color="var(--primary)" />
                </div>
            </div>

            <div style={{
                transform: `translateY(${pullChange}px)`,
                transition: refreshing || pullChange === 0 ? 'transform 0.3s ease-out' : 'none'
            }}>
                {children}
            </div>
        </div>
    );
}
