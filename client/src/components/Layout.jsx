import { useState } from 'react';
import { NavLink } from 'react-router-dom';
import { Home, FolderSearch, Activity, Sliders, Menu, X } from 'lucide-react';

export default function Layout({ children }) {
    const [isMobileMenuOpen, setIsMobileMenuOpen] = useState(false);

    const navItems = [
        { to: '/dashboard', label: 'Dashboard', icon: Home },
        { to: '/browse', label: 'Remote Browser', icon: FolderSearch },
        { to: '/jobs', label: 'Job Monitor', icon: Activity },
        { to: '/settings', label: 'Settings', icon: Sliders },
    ];

    return (
        <div className="app-container">
            {/* Mobile Menu Button */}
            <button
                className="mobile-menu-btn"
                onClick={() => setIsMobileMenuOpen(!isMobileMenuOpen)}
                aria-label="Toggle menu"
            >
                {isMobileMenuOpen ? <X size={24} /> : <Menu size={24} />}
            </button>

            {/* Mobile Backdrop */}
            {isMobileMenuOpen && (
                <div
                    className="mobile-backdrop"
                    onClick={() => setIsMobileMenuOpen(false)}
                />
            )}

            <aside className={`sidebar ${isMobileMenuOpen ? 'open' : ''}`}>
                <div style={{ display: 'flex', alignItems: 'center', gap: '12px', padding: '0 16px 24px' }}>
                    <img src="/logo.svg" alt="SyncPane" style={{ width: 40, height: 40 }} />
                    <h1 style={{ fontSize: '1.2rem', fontWeight: 800 }}>SyncPane</h1>
                </div>

                <nav style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
                    {navItems.map(({ to, label, icon: Icon }) => (
                        <NavLink
                            key={to}
                            to={to}
                            className={({ isActive }) => `nav-item ${isActive ? 'active' : ''}`}
                            onClick={() => setIsMobileMenuOpen(false)}
                        >
                            <Icon size={20} />
                            <span>{label}</span>
                        </NavLink>
                    ))}
                </nav>
            </aside>
            <main className="main-content">
                {children}
            </main>
        </div>
    );
}
