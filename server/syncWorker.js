const fs = require('fs');
const path = require('path');
const db = require('./database');
const { connect } = require('./sshManager');
const uuid = require('uuid');

const CONCURRENCY = 1; // Number of simultaneous jobs
let isRunning = false;

async function processQueue() {
    if (isRunning) return;
    isRunning = true;

    try {
        // Get next queued job
        const job = db.prepare('SELECT * FROM jobs WHERE status = ? ORDER BY priority DESC, created_at ASC LIMIT 1').get('queued');

        if (!job) {
            isRunning = false;
            return;
        }

        console.log(`Starting job ${job.id} (${job.type})`);

        // Mark running
        db.prepare('UPDATE jobs SET status = ?, started_at = CURRENT_TIMESTAMP WHERE id = ?').run('running', job.id);

        try {
            if (job.type === 'sync') {
                await performSync(job);
            }

            // Check if job was paused or cancelled during execution
            const currentJob = db.prepare('SELECT status FROM jobs WHERE id = ?').get(job.id);

            // Only mark as completed if it's still marked as running (meaning it finished naturally)
            if (currentJob.status === 'running') {
                // Mark completed
                db.prepare('UPDATE jobs SET status = ?, completed_at = CURRENT_TIMESTAMP, log = ? WHERE id = ?')
                    .run('completed', 'Sync completed successfully', job.id);

                // Update item status
                db.prepare('UPDATE sync_items SET status = ?, last_synced_at = CURRENT_TIMESTAMP, error_message = NULL WHERE id = ?')
                    .run('synced', job.sync_item_id);
            }

        } catch (err) {
            console.error(`Job ${job.id} failed:`, err);
            // Mark failed
            db.prepare('UPDATE jobs SET status = ?, completed_at = CURRENT_TIMESTAMP, log = ? WHERE id = ?')
                .run('failed', err.message, job.id);

            // Update item status
            db.prepare('UPDATE sync_items SET status = ?, error_message = ? WHERE id = ?')
                .run('error', err.message, job.sync_item_id);
        }

    } catch (err) {
        console.error('Worker error:', err);
    } finally {
        isRunning = false;
        // Check for more jobs immediately
        setTimeout(processQueue, 1000);
    }
}

const { getFolderSizes } = require('./sshManager');

// Job Control Map: jobId -> { cancel: bool, pause: bool }
const activeJobControllers = new Map();

function cancelJob(jobId) {
    const job = db.prepare('SELECT status FROM jobs WHERE id = ?').get(jobId);
    if (!job) return;

    // Check if it's in memory
    const isActive = activeJobControllers.has(jobId);

    if (job.status === 'running' || job.status === 'pausing' || job.status === 'cancelling') {
        if (isActive) {
            // Normal graceful cancel
            activeJobControllers.get(jobId).cancel = true;
            db.prepare("UPDATE jobs SET status = 'cancelling' WHERE id = ?").run(jobId);
        } else {
            // Orphaned job (server restarted?) -> Force cancel
            db.prepare("UPDATE jobs SET status = 'cancelled', completed_at = CURRENT_TIMESTAMP, log = 'Force cancelled (orphaned)' WHERE id = ?").run(jobId);
        }
    } else {
        // If queued or paused, just mark cancelled
        db.prepare("UPDATE jobs SET status = 'cancelled', completed_at = CURRENT_TIMESTAMP, log = 'Cancelled by user' WHERE id = ?").run(jobId);
    }
}

function pauseJob(jobId) {
    const job = db.prepare('SELECT status FROM jobs WHERE id = ?').get(jobId);
    if (!job) return;

    if (job.status === 'running') {
        db.prepare("UPDATE jobs SET status = 'pausing' WHERE id = ?").run(jobId);
        if (activeJobControllers.has(jobId)) {
            activeJobControllers.get(jobId).pause = true;
        }
    }
}

function resumeJob(jobId) {
    const job = db.prepare('SELECT status FROM jobs WHERE id = ?').get(jobId);
    if (!job) return;

    if (job.status === 'paused') {
        db.prepare("UPDATE jobs SET status = 'queued' WHERE id = ?").run(jobId);
    }
}

async function performSync(job) {
    const item = db.prepare('SELECT * FROM sync_items WHERE id = ?').get(job.sync_item_id);
    if (!item) throw new Error('Sync item not found');

    if (!item.active && job.priority < 10) throw new Error('Sync item is inactive');

    // Check for local deletion (unless manual sync override)
    if (item.status === 'synced' && !fs.existsSync(item.local_path) && job.priority < 10) {
        db.prepare('UPDATE sync_items SET active = 0, status = ? WHERE id = ?').run('unsynced - local missing', item.id);
        throw new Error('Local file/folder missing. Sync disabled due to local deletion.');
    }

    // Register Controller
    activeJobControllers.set(job.id, { cancel: false, pause: false });

    const conn = await connect();

    // Helper to check interruption
    const checkInterruption = () => {
        const controller = activeJobControllers.get(job.id);
        if (!controller) return null;
        if (controller.cancel) {
            return 'cancelled';
        }
        if (controller.pause) {
            return 'paused';
        }
        return null;
    };

    return new Promise((resolve, reject) => {
        conn.sftp(async (err, sftp) => {
            if (err) {
                conn.end();
                activeJobControllers.delete(job.id);
                return reject(err);
            }

            try {
                // Initialize Progress Tracking
                const tracker = {
                    totalBytes: 0,
                    processedBytes: 0,
                    startTime: Date.now(),
                    lastUpdate: 0,
                    jobId: job.id,
                    failedItems: []
                };

                // 1. Generate Sync Plan (Diff)
                // Check interruption before heavy scan?
                if (checkInterruption()) throw new Error('Interrupted');

                const plan = await generateSyncPlan(sftp, item.remote_path, item.local_path, item.type, tracker.failedItems);

                tracker.totalBytes = plan.totalBytes;

                console.log(`[Job ${job.id}] Sync Plan Generated: ${plan.files.length} files to sync (${plan.totalBytes} bytes).`);
                console.log(`[Job ${job.id}] Target: ${item.local_path} <- ${item.remote_path}`);

                // Update DB with total (this is now the transfer size)
                db.prepare('UPDATE jobs SET total_bytes = ? WHERE id = ?').run(tracker.totalBytes, job.id);

                // 2. Perform Sync based on Plan
                updateProgress(tracker, true); // Initial update (0/Total)

                for (const fileTask of plan.files) {
                    const status = checkInterruption();
                    if (status) {
                        // Handle Stop (Cancellation/Pause check BEFORE file start)
                        if (status === 'cancelled') {
                            db.prepare("UPDATE jobs SET status = 'cancelled', completed_at = CURRENT_TIMESTAMP, log = 'Cancelled by user' WHERE id = ?").run(job.id);
                            throw new Error('Cancelled by user');
                        }
                        if (status === 'paused') {
                            db.prepare("UPDATE jobs SET status = 'paused', log = 'Paused by user' WHERE id = ?").run(job.id);
                            throw new Error('Paused by user');
                        }
                    }

                    // Pass checkInterruption callback to syncFile
                    await syncFile(sftp, fileTask, tracker, checkInterruption);
                }

                // Final update
                updateProgress(tracker, true);

                // Check for failures
                if (tracker.failedItems.length > 0) {
                    db.prepare('UPDATE jobs SET failed_items = ? WHERE id = ?')
                        .run(JSON.stringify(tracker.failedItems), job.id);
                }

                conn.end();
                activeJobControllers.delete(job.id);
                resolve();
            } catch (syncErr) {
                conn.end();
                activeJobControllers.delete(job.id);

                if (syncErr.message === 'Cancelled by user') {
                    db.prepare("UPDATE jobs SET status = 'cancelled', completed_at = CURRENT_TIMESTAMP, log = 'Cancelled by user' WHERE id = ?").run(job.id);
                    return resolve();
                } else if (syncErr.message === 'Paused by user') {
                    db.prepare("UPDATE jobs SET status = 'paused', log = 'Paused by user' WHERE id = ?").run(job.id);
                    return resolve();
                }

                reject(syncErr);
            }
        });
    });
}

// Helpers

function shouldSync(remoteAttrs, localPath) {
    if (!fs.existsSync(localPath)) return 'missing';

    // Safety check for file stat
    try {
        const stats = fs.statSync(localPath);

        // 1. Check Size
        if (stats.size !== remoteAttrs.size) return 'size_mismatch';

        // 2. Check Mtime (allow 2 second variance)
        // remote mtime is in seconds (unix). local mtimeMs is milliseconds.
        const remoteTime = remoteAttrs.mtime;
        const localTime = Math.floor(stats.mtimeMs / 1000);

        if (Math.abs(remoteTime - localTime) > 2) return 'time_mismatch';

        return null; // Files are identical
    } catch (e) {
        return 'error'; // If assume error means explicit sync needed
    }
}

async function generateSyncPlan(sftp, remotePath, localPath, type, failedItems) {
    const plan = { files: [], totalBytes: 0 };

    if (type === 'file') {
        try {
            const stat = await sftpStats(sftp, remotePath);
            const reason = shouldSync(stat, localPath);
            if (reason) {
                plan.files.push({ remotePath, localPath, size: stat.size, mtime: stat.mtime, atime: stat.atime, reason });
                plan.totalBytes += stat.size;
            }
        } catch (e) {
            failedItems.push({ path: remotePath, error: e.message });
        }
    } else {
        await scanFolder(sftp, remotePath, localPath, plan, failedItems);
    }
    return plan;
}

async function scanFolder(sftp, remoteDir, localDir, plan, failedItems) {
    let list;
    try {
        list = await new Promise((resolve, reject) => {
            sftp.readdir(remoteDir, (err, list) => {
                if (err) reject(err);
                else resolve(list);
            });
        });
    } catch (e) {
        failedItems.push({ path: remoteDir, error: 'Scan failed: ' + e.message });
        return;
    }

    for (const item of list) {
        if (item.filename === '.DS_Store') continue;
        const rPath = `${remoteDir}/${item.filename}`;
        const lPath = path.join(localDir, item.filename);

        if (item.attrs.isDirectory()) {
            await scanFolder(sftp, rPath, lPath, plan, failedItems);
        } else {
            const reason = shouldSync(item.attrs, lPath);
            if (reason) {
                plan.files.push({
                    remotePath: rPath,
                    localPath: lPath,
                    size: item.attrs.size,
                    mtime: item.attrs.mtime,
                    atime: item.attrs.atime,
                    reason
                });
                plan.totalBytes += item.attrs.size;
            }
        }
    }
}

// Helper to get stats wrapper
function sftpStats(sftp, path) {
    return new Promise((resolve, reject) => {
        sftp.stat(path, (err, stats) => {
            if (err) reject(err);
            else resolve(stats);
        });
    });
}

function updateProgress(tracker, force = false) {
    const now = Date.now();
    if (!force && now - tracker.lastUpdate < 1000) return; // Throttle to 1s

    const elapsedSeconds = (now - tracker.startTime) / 1000;
    const speed = elapsedSeconds > 0 ? tracker.processedBytes / elapsedSeconds : 0; // Bytes/sec
    const remainingBytes = tracker.totalBytes - tracker.processedBytes;
    const eta = speed > 0 ? Math.ceil(remainingBytes / speed) : null;

    db.prepare(`
        UPDATE jobs
        SET processed_bytes = ?, current_speed = ?, eta_seconds = ?
        WHERE id = ?
    `).run(tracker.processedBytes, speed, eta, tracker.jobId);

    tracker.lastUpdate = now;
}

function syncFile(sftp, task, tracker, checkInterruption) {
    return new Promise((resolve, reject) => {
        const { remotePath, localPath, atime, mtime } = task;
        const dir = path.dirname(localPath);

        if (!fs.existsSync(dir)) {
            try {
                fs.mkdirSync(dir, { recursive: true });
            } catch (fsErr) {
                tracker.failedItems.push({ path: remotePath, error: 'Local mkdir failed: ' + fsErr.message });
                return resolve();
            }
        }

        // Use Streams instead of fastGet for interrupt capability
        let readStream;
        let writeStream;
        let completed = false;
        let interrupted = false;

        // Check for partial transfer to resume
        let startOffset = 0;
        let flags = 'w';
        try {
            if (fs.existsSync(localPath)) {
                const stats = fs.statSync(localPath);
                if (stats.size < task.size) { // remote size is in task object
                    startOffset = stats.size;
                    flags = 'a';
                    tracker.processedBytes += startOffset; // Count already downloaded bytes
                    console.log(`Resuming download for ${remotePath} from offset ${startOffset}`);
                }
            }
        } catch (e) {
            console.warn('Resume check failed, starting fresh:', e);
        }

        try {
            // Updated to use start option for resume
            const resumeMsg = startOffset > 0 ? `Resuming from ${startOffset} bytes` : 'Starting fresh';
            console.log(`[Job ${tracker.jobId}] Downloading: ${remotePath} (${task.size} bytes) - ${resumeMsg}`);

            readStream = sftp.createReadStream(remotePath, { start: startOffset });
            writeStream = fs.createWriteStream(localPath, { flags });
        } catch (streamErr) {
            tracker.failedItems.push({ path: remotePath, error: 'Stream creation failed: ' + streamErr.message });
            return resolve();
        }

        readStream.on('data', (chunk) => {
            // Check for interruption during transfer
            const status = checkInterruption ? checkInterruption() : null;
            if (status) {
                interrupted = true;
                readStream.destroy();
                writeStream.destroy();

                // We throw up to the parent loop to handle DB status updates
                if (status === 'cancelled') {
                    console.log(`[Job ${tracker.jobId}] Transfer cancelled: ${remotePath}`);
                    reject(new Error('Cancelled by user'));
                }
                else if (status === 'paused') {
                    console.log(`[Job ${tracker.jobId}] Transfer paused: ${remotePath}`);
                    reject(new Error('Paused by user'));
                }
                return;
            }

            tracker.processedBytes += chunk.length;
            updateProgress(tracker);
        });

        readStream.on('error', (err) => {
            if (interrupted) return; // Ignore errors caused by destroy()
            tracker.failedItems.push({ path: remotePath, error: err.message });
            writeStream.end();
            resolve();
        });

        writeStream.on('error', (err) => {
            if (interrupted) return;
            tracker.failedItems.push({ path: remotePath, error: 'Write error: ' + err.message });
            readStream.destroy();
            resolve();
        });

        writeStream.on('finish', () => {
            if (interrupted) return;
            completed = true;

            // SUCCESS - Update mtime to match remote!
            try {
                fs.utimesSync(localPath, atime, mtime);
            } catch (timeErr) {
                console.warn(`Failed to set timestamp for ${localPath}:`, timeErr);
            }
            console.log(`[Job ${tracker.jobId}] Completed: ${remotePath}`);
            resolve();
        });

        readStream.pipe(writeStream);
    });
}


// Helper to check diff status
async function checkItemDiff(itemId) {
    const item = db.prepare('SELECT * FROM sync_items WHERE id = ?').get(itemId);
    if (!item) throw new Error('Item not found');

    // 0. Pre-check for local deletion
    if (!fs.existsSync(item.local_path)) {
        const msg = 'Local file/folder missing. Sync disabled due to local deletion.';
        db.prepare('UPDATE sync_items SET active = 0, status = ?, error_message = ? WHERE id = ?')
            .run('error', msg, item.id);
        return { status: 'local_missing', error: msg };
    }

    let conn;
    try {
        conn = await connect();
        const plan = await new Promise((resolve, reject) => {
            conn.sftp(async (err, sftp) => {
                if (err) return reject(err);
                try {
                    // We use generateSyncPlan which already does the heavy lifting of recursive diff
                    const failedItems = [];
                    const plan = await generateSyncPlan(sftp, item.remote_path, item.local_path, item.type, failedItems);
                    resolve(plan);
                } catch (e) {
                    reject(e);
                }
            });
        });

        conn.end();

        if (plan.files.length > 0) {
            // If restored but outdated, still clear the specific missing error
            if (item.error_message === 'Local file/folder missing. Sync disabled due to local deletion.') {
                db.prepare("UPDATE sync_items SET status = 'synced', error_message = NULL WHERE id = ?").run(item.id);
            }
            return { status: 'outdated', diffCount: plan.files.length, diffSize: plan.totalBytes, diffFiles: plan.files };
        } else {
            // Fully synced + recovered
            if (item.error_message === 'Local file/folder missing. Sync disabled due to local deletion.') {
                db.prepare("UPDATE sync_items SET status = 'synced', error_message = NULL WHERE id = ?").run(item.id);
            }
            return { status: 'synced' };
        }

    } catch (err) {
        if (conn) conn.end();
        console.error(`Check status failed for ${item.id}:`, err);
        return { status: 'error', error: err.message };
    }
}

// Start the loop
setInterval(processQueue, 2000);

module.exports = { processQueue, cancelJob, pauseJob, resumeJob, checkItemDiff };
