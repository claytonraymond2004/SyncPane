const express = require('express');
const fs = require('fs').promises;
const cors = require('cors');
const path = require('path');
const uuid = require('uuid');
require('dotenv').config();

const db = require('./database');
const { encrypt } = require('./encryption');
const { connect, listRemote, getConnectionConfig } = require('./sshManager');
const { initScheduler, queueAllActive, updateSchedule } = require('./scheduler');

// Start Worker (just by requiring it, the interval starts)
const { processQueue, cancelJob, pauseJob, resumeJob, preemptJob, checkItemDiff } = require('./syncWorker');

const app = express();
const PORT = process.env.PORT || 3001;

app.use(cors());
app.use(express.json());

// --- API Routes ---

// 1. Configuration

app.get('/api/settings', (req, res) => {
    try {
        const schedule = db.prepare('SELECT value FROM config WHERE key = ?').get('sync_schedule');
        const globalEnabled = db.prepare('SELECT value FROM config WHERE key = ?').get('global_sync_enabled');

        res.json({
            sync_schedule: schedule ? schedule.value : '0 * * * *',
            global_sync_enabled: globalEnabled ? globalEnabled.value === 'true' : true,
            connection_timeout_minutes: parseInt(db.prepare('SELECT value FROM config WHERE key = ?').get('connection_timeout_minutes')?.value || '60')
        });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

app.post('/api/settings', (req, res) => {
    try {
        const { sync_schedule, global_sync_enabled, connection_timeout_minutes } = req.body;

        if (sync_schedule) {
            db.prepare('INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)').run('sync_schedule', sync_schedule);
            updateSchedule(sync_schedule);
        }

        if (global_sync_enabled !== undefined) {
            db.prepare('INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)').run('global_sync_enabled', String(global_sync_enabled));
        }

        if (connection_timeout_minutes !== undefined) {
            db.prepare('INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)').run('connection_timeout_minutes', String(connection_timeout_minutes));
        }

        res.json({ success: true });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

app.get('/api/config', (req, res) => {
    // Return config but mask secrets
    const config = getConnectionConfig();
    res.json({
        host: config.host,
        port: config.port,
        username: config.username,
        hasPassword: !!config.password,

        hasKey: !!config.privateKey,
        isEnv: !!config.isEnv
    });
});

// Helper to prepare config for testing (merges current DB config with incoming changes)
function prepareTestConfig(body) {
    const current = getConnectionConfig(); // Decrypted from DB

    // Helper: Any explicit value in body overrides current, even empty string.
    // If undefined in body, keep current.
    const getValue = (key) => (body[key] !== undefined ? body[key] : current[key]);

    return {
        host: getValue('host'),
        port: body.port ? parseInt(body.port) : current.port,
        username: getValue('username'),
        password: getValue('password'),
        privateKey: getValue('privateKey')
    };
}

app.post('/api/config/test', async (req, res) => {
    try {
        const config = prepareTestConfig(req.body);
        const conn = await connect(config);
        conn.end();
        res.json({ success: true, message: 'Connection successful' });
    } catch (err) {
        res.status(400).json({ success: false, error: err.message });
    }
});

app.post('/api/config', async (req, res) => {
    const { host, port, username, password, privateKey, skipTest } = req.body;

    try {
        // If not skipping test, verify first
        if (!skipTest) {
            const config = prepareTestConfig(req.body);
            const conn = await connect(config);
            conn.end();
        }

        // Save to DB
        const stmt = db.prepare('INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)');
        const transaction = db.transaction(() => {
            if (host) stmt.run('host', host);
            if (port) stmt.run('port', String(port));
            if (username) stmt.run('username', username);

            // Special handling for secrets: empty string means DELETE
            if (password !== undefined) {
                if (password === '') db.prepare("DELETE FROM config WHERE key = 'password'").run();
                else stmt.run('password', encrypt(password));
            }

            if (privateKey !== undefined) {
                if (privateKey === '') db.prepare("DELETE FROM config WHERE key = 'privateKey'").run();
                else stmt.run('privateKey', encrypt(privateKey));
            }
        });

        transaction();

        // If we just saved new valid config, we can optionally re-connect the global connection pool if we had one
        // But here we just return success.
        res.json({ success: true, message: 'Configuration saved.' });

    } catch (err) {
        console.error('Config save error:', err);
        // If it was a connection error during verification
        if (!skipTest && (
            err.level === 'client-socket' ||
            err.level === 'client-authentication' ||
            err.code === 'ECONNREFUSED' ||
            err.message.includes('SSH') ||
            err.message.includes('authentication methods failed')
        )) {
            res.status(400).json({ success: false, error: err.message, type: 'CONNECTION_FAILED' });
        } else {
            res.status(500).json({ success: false, error: err.message });
        }
    }
});

app.get('/api/sync-locations', (req, res) => {
    const rows = db.prepare('SELECT * FROM sync_locations').all();
    res.json(rows);
});

app.post('/api/sync-locations', (req, res) => {
    const { path, label } = req.body;
    try {
        db.prepare('INSERT INTO sync_locations (path, label) VALUES (?, ?)').run(path, label || '');
        res.json({ success: true });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

app.delete('/api/sync-locations/:id', (req, res) => {
    try {
        db.prepare('DELETE FROM sync_locations WHERE id = ?').run(req.params.id);
        res.json({ success: true });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// 1b. Local Browser (for config)
app.get('/api/local/list', async (req, res) => {
    const localPath = req.query.path || '/app'; // Default to /app
    try {
        const items = await fs.readdir(localPath, { withFileTypes: true });
        const files = await Promise.all(items
            .filter(item => item.name !== '.DS_Store')
            .map(async (item) => {
                const fullPath = path.join(localPath, item.name);
                let size = 0;
                if (item.isFile()) {
                    try {
                        const stats = await fs.stat(fullPath);
                        size = stats.size;
                    } catch (e) { /* ignore */ }
                }
                return {
                    name: item.name,
                    type: item.isDirectory() ? 'folder' : 'file',
                    path: fullPath,
                    size: size
                };
            }));

        // Sort folders first
        files.sort((a, b) => {
            if (a.type === b.type) return a.name.localeCompare(b.name);
            return a.type === 'folder' ? -1 : 1;
        });

        res.json(files);
    } catch (err) {
        console.error('List local error:', err);
        res.status(500).json({ error: err.message });
    }
});

// 2. Remote Browser
app.get('/api/remote/list', async (req, res) => {
    const remotePath = req.query.path || '/';
    try {
        const files = await listRemote(remotePath);
        res.json(files);
    } catch (err) {
        if (err.message !== 'Missing SSH configuration') {
            console.error('List remote error:', err);
        }
        res.status(500).json({ error: err.message });
    }
});

// 2b. Remote Folder Sizes
app.post('/api/remote/folders/sizes', async (req, res) => {
    const { paths } = req.body;
    try {
        const { getFolderSizes } = require('./sshManager');
        const sizes = await getFolderSizes(paths);
        res.json(sizes);
    } catch (err) {
        console.error('Folder sizes error:', err);
        // Don't fail the whole UI for size calculation errors
        res.status(500).json({ error: err.message });
    }
});

// 3. Sync Items
app.get('/api/sync', (req, res) => {
    const rows = db.prepare("SELECT * FROM sync_items WHERE status != 'deleted'").all();
    const jobs = db.prepare("SELECT * FROM jobs WHERE status IN ('running', 'queued', 'paused', 'pausing')").all();

    // Map jobs to items
    const rowsWithJobs = rows.map(item => {
        const itemJobs = jobs.filter(j => j.sync_item_id === item.id);
        // Prioritize paused jobs so the UI shows the Resume option if available
        const activeJob = itemJobs.find(j => j.status === 'paused') || itemJobs[0];
        return { ...item, activeJob };
    });

    res.json(rowsWithJobs);
});

app.post('/api/sync', (req, res) => {
    const { remotePath, localPath, type } = req.body;
    try {
        // Check if exists (including deleted)
        const existing = db.prepare('SELECT * FROM sync_items WHERE remote_path = ? AND local_path = ?').get(remotePath, localPath);

        if (existing) {
            if (existing.status === 'deleted') {
                // Reactivate
                db.prepare("UPDATE sync_items SET status = 'pending', active = 1, error_message = NULL WHERE id = ?").run(existing.id);

                // Trigger initial sync job
                const jobId = uuid.v4();
                db.prepare('INSERT INTO jobs (id, type, sync_item_id, status) VALUES (?, ?, ?, ?)').run(jobId, 'sync', existing.id, 'queued');

                return res.json({ success: true, id: existing.id, message: 'Restored deleted sync item' });
            } else {
                return res.status(409).json({ error: 'Sync item already exists' });
            }
        }

        const info = db.prepare('INSERT INTO sync_items (remote_path, local_path, type, active) VALUES (?, ?, ?, ?)')
            .run(remotePath, localPath, type, req.body.active !== false ? 1 : 0);

        // Trigger initial sync job
        const jobId = uuid.v4();
        db.prepare('INSERT INTO jobs (id, type, sync_item_id, status) VALUES (?, ?, ?, ?)').run(jobId, 'sync', info.lastInsertRowid, 'queued');

        res.json({ success: true, id: info.lastInsertRowid });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

app.delete('/api/sync/:id', async (req, res) => {
    try {
        const id = req.params.id;

        // Cancel active jobs for this item
        const activeJobs = db.prepare("SELECT id FROM jobs WHERE sync_item_id = ? AND status IN ('queued', 'running', 'paused', 'pausing')").all(id);
        activeJobs.forEach(job => {
            console.log(`Cancelling active job ${job.id} for deleted sync item ${id}`);
            try {
                cancelJob(job.id);
            } catch (err) {
                console.error(`Failed to cancel job ${job.id}:`, err);
            }
        });

        if (req.query.deleteFiles === 'true') {
            const item = db.prepare('SELECT local_path FROM sync_items WHERE id = ?').get(id);
            if (item && item.local_path) {
                try {
                    await fs.rm(item.local_path, { recursive: true, force: true });
                    console.log(`Deleted local files at ${item.local_path}`);
                } catch (e) {
                    console.error('Failed to delete local files:', e);
                }
            }
        }

        // Soft delete: Mark as deleted and inactive
        // We do NOT delete from jobs table, so history is preserved.
        const result = db.prepare("UPDATE sync_items SET status = 'deleted', active = 0 WHERE id = ?").run(id);

        console.log(`Soft deleted sync item ${id}. Changes: ${result.changes}`);
        res.json({ success: true });
    } catch (err) {
        console.error('Delete error:', err);
        res.status(500).json({ error: err.message });
    }
});

app.post('/api/sync/:id/toggle', (req, res) => {
    try {
        const { active } = req.body; // true/false
        db.prepare('UPDATE sync_items SET active = ? WHERE id = ?').run(active ? 1 : 0, req.params.id);
        res.json({ success: true });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

app.get('/api/sync/:id/status', async (req, res) => {
    try {
        const result = await checkItemDiff(req.params.id);
        res.json(result);
    } catch (err) {
        res.status(500).json({ status: 'error', error: err.message });
    }
});

// 4. Jobs
app.get('/api/jobs', (req, res) => {
    const page = parseInt(req.query.page) || 1;
    const limit = parseInt(req.query.limit) || 25;
    const search = req.query.search || '';
    const offset = (page - 1) * limit;

    try {
        // Base Query parts
        let whereClause = '';
        const params = [];

        if (search) {
            whereClause = `
                WHERE 
                jobs.status LIKE ? OR 
                jobs.type LIKE ? OR 
                jobs.log LIKE ? OR 
                jobs.failed_items LIKE ? OR
                sync_items.remote_path LIKE ?
            `;
            const term = `%${search}%`;
            params.push(term, term, term, term, term);
        }

        // 1. Get Data
        const dataQuery = `
            SELECT jobs.*, sync_items.local_path, sync_items.remote_path 
            FROM jobs 
            LEFT JOIN sync_items ON jobs.sync_item_id = sync_items.id
            ${whereClause}
            ORDER BY COALESCE(jobs.started_at, jobs.created_at) DESC 
            LIMIT ? OFFSET ?
        `;

        const rows = db.prepare(dataQuery).all(...params, limit, offset);

        // 2. Get Total Count
        const countQuery = `
            SELECT COUNT(*) as count 
            FROM jobs 
            LEFT JOIN sync_items ON jobs.sync_item_id = sync_items.id
            ${whereClause}
        `;
        const total = db.prepare(countQuery).get(...params).count;

        // 3. Get Global Stats for UI Actions (Pause All / Resume All)
        // We need to know if *any* jobs exist in these states, regardless of search filter
        const stats = db.prepare(`
            SELECT 
                SUM(CASE WHEN status IN ('running', 'queued', 'pausing') THEN 1 ELSE 0 END) as activeCount,
                SUM(CASE WHEN status IN ('paused', 'pausing') THEN 1 ELSE 0 END) as pausedCount
            FROM jobs
        `).get();

        res.json({
            data: rows,
            pagination: {
                page,
                limit,
                total,
                totalPages: Math.ceil(total / limit)
            },
            stats: {
                active: stats.activeCount || 0,
                paused: stats.pausedCount || 0
            }
        });
    } catch (err) {
        console.error('Jobs API error:', err);
        res.status(500).json({ error: err.message });
    }
});

app.post('/api/jobs/manual', (req, res) => {
    const { syncItemId } = req.body;
    try {
        // Auto-cancel any PAUSED jobs for this item to avoid duplicates/stale state
        const pausedJobs = db.prepare("SELECT id FROM jobs WHERE sync_item_id = ? AND status = 'paused'").all(syncItemId);
        pausedJobs.forEach(job => {
            console.log(`Auto-cancelling paused job ${job.id} due to manual sync request`);
            cancelJob(job.id);
        });

        const jobId = uuid.v4();
        db.prepare('INSERT INTO jobs (id, type, sync_item_id, status, priority) VALUES (?, ?, ?, ?, ?)')
            .run(jobId, 'sync', syncItemId, 'queued', 10); // Higher priority for manual

        // Clear previous error on the item so it looks "fresh" immediately
        db.prepare("UPDATE sync_items SET error_message = NULL WHERE id = ?").run(syncItemId);

        res.json({ success: true, jobId });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});



app.post('/api/jobs/:id/priority', (req, res) => {
    const { priority } = req.body;
    const targetId = req.params.id;

    try {
        // 1. Pause other running jobs to free up bandwidth/slots
        const runningJobs = db.prepare("SELECT id FROM jobs WHERE status IN ('running', 'pausing') AND id != ?").all(targetId);
        runningJobs.forEach(job => {
            console.log(`Preempting job ${job.id} to prioritize ${targetId}`);
            try {
                preemptJob(job.id);
            } catch (e) {
                console.error(`Failed to preempt job ${job.id}:`, e);
            }
        });

        // 2. Update priority - Ensure it becomes the highest
        const maxPriority = db.prepare('SELECT MAX(priority) as maxP FROM jobs').get().maxP || 10;
        const newPriority = maxPriority + 1;

        db.prepare('UPDATE jobs SET priority = ? WHERE id = ?').run(newPriority, targetId);
        console.log(`Updated job ${targetId} priority to ${newPriority}`);

        // 3. Auto-resume if paused (so it starts immediately)
        const targetJob = db.prepare('SELECT status FROM jobs WHERE id = ?').get(targetId);
        if (targetJob && targetJob.status === 'paused') {
            console.log(`Auto-resuming prioritized job ${targetId}`);
            resumeJob(targetId);
        }

        res.json({ success: true });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

app.post('/api/jobs/:id/cancel', (req, res) => {
    try {
        cancelJob(req.params.id);
        res.json({ success: true });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

app.post('/api/jobs/:id/pause', (req, res) => {
    try {
        pauseJob(req.params.id);
        res.json({ success: true });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

app.post('/api/jobs/:id/resume', (req, res) => {
    try {
        resumeJob(req.params.id);
        res.json({ success: true });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

app.post('/api/jobs/pause-all', (req, res) => {
    try {
        // 1. Pause running/pausing jobs (individually to trigger controllers)
        const activeJobs = db.prepare("SELECT id FROM jobs WHERE status IN ('running', 'pausing')").all();
        activeJobs.forEach(job => {
            try { pauseJob(job.id); } catch (e) { console.error(`Failed to pause job ${job.id}`, e); }
        });

        // 2. Pause queued jobs (bulk update is safe for queued)
        db.prepare("UPDATE jobs SET status = 'paused', log = 'Paused by user (Global)' WHERE status = 'queued'").run();

        res.json({ success: true });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

app.post('/api/jobs/resume-all', (req, res) => {
    try {
        // 1. Resume paused/pausing jobs
        // We need to use resumeJob for all to ensure controllers are updated if 'pausing'
        const pausedJobs = db.prepare("SELECT id FROM jobs WHERE status IN ('paused', 'pausing')").all();
        pausedJobs.forEach(job => {
            try { resumeJob(job.id); } catch (e) { console.error(`Failed to resume job ${job.id}`, e); }
        });

        res.json({ success: true });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});


// Serve frontend in production
if (process.env.NODE_ENV === 'production') {
    app.use(express.static(path.join(__dirname, '../client/dist')));
    app.get('*', (req, res) => {
        res.sendFile(path.join(__dirname, '../client/dist/index.html'));
    });
}

// Start
initScheduler();

app.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
});
