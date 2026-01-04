
// Use VITE_API_URL environment variable if set (e.g. via Docker Compose dev)
// Otherwise fallback to empty string (relative path) for production builds
export const API_BASE = import.meta.env.VITE_API_URL || '';
