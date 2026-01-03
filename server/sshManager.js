const { Client } = require('ssh2');
const db = require('./database');
const { decrypt } = require('./encryption');

function getConnectionConfig() {
    const stmt = db.prepare('SELECT * FROM config WHERE key IN (?, ?, ?, ?, ?)');
    const rows = stmt.all('host', 'port', 'username', 'password', 'privateKey');

    const config = {};
    rows.forEach(row => {
        if (row.key === 'password' || row.key === 'privateKey') {
            config[row.key] = decrypt(row.value);
        } else if (row.key === 'port') {
            config[row.key] = parseInt(row.value, 10);
        } else {
            config[row.key] = row.value;
        }
    });

    // If env vars are set, they override DB (docker .env use case)
    if (process.env.REMOTE_HOST) config.host = process.env.REMOTE_HOST;
    if (process.env.REMOTE_PORT) config.port = parseInt(process.env.REMOTE_PORT, 10);
    if (process.env.REMOTE_USER) config.username = process.env.REMOTE_USER;
    if (process.env.REMOTE_PASSWORD) config.password = process.env.REMOTE_PASSWORD; // Assumed plain in env for docker
    if (process.env.SSH_KEY_PATH) config.privateKey = require('fs').readFileSync(process.env.SSH_KEY_PATH);

    // Keep-Alive Settings to detect dropped connections
    config.keepaliveInterval = 10000; // Send keepalive every 10 seconds
    config.keepaliveCountMax = 3;     // Disconnect after 3 failed keepalives (30s total)

    return config;
}

function connect(overrideConfig = null) {
    return new Promise((resolve, reject) => {
        const config = overrideConfig || getConnectionConfig();
        if (!config.host || !config.username) {
            return reject(new Error('Missing SSH configuration'));
        }

        const conn = new Client();
        conn.on('ready', () => {
            resolve(conn);
        }).on('error', (err) => {
            reject(err);
        }).connect(config);
    });
}

function listRemote(path = '/') {
    return new Promise(async (resolve, reject) => {
        let conn;
        try {
            conn = await connect();
            conn.sftp((err, sftp) => {
                if (err) {
                    conn.end();
                    return reject(err);
                }
                sftp.readdir(path, (err, list) => {
                    if (err) {
                        conn.end();
                        return reject(err);
                    }
                    conn.end();

                    // Filter out .DS_Store
                    const filteredList = list.filter(item => item.filename !== '.DS_Store');

                    // Format output
                    const formatted = filteredList.map(item => ({
                        name: item.filename,
                        type: item.attrs.isDirectory() ? 'folder' : 'file',
                        size: item.attrs.size,
                        mtime: item.attrs.mtime,
                        path: path === '/' ? `/${item.filename}` : `${path}/${item.filename}`
                    }));
                    resolve(formatted);
                });
            });
        } catch (err) {
            if (conn) conn.end();
            reject(err);
        }
    });
}

function escapeShellArg(arg) {
    return `'${arg.replace(/'/g, "'\\''")}'`;
}

function getFolderSizes(paths) {
    return new Promise(async (resolve, reject) => {
        if (!paths || paths.length === 0) return resolve({});

        let conn;
        try {
            conn = await connect();

            // Strategy: Try both the raw path AND the path prefixed with /volume1 (common for Synology SFTP chroot)
            // du will error on the missing ones, but report the existing ones.
            // We'll map the results back to the original requested paths.

            const rawPaths = paths;
            const vol1Paths = paths.map(p => p.startsWith('/') ? `/volume1${p}` : `/volume1/${p}`);

            // Combine and unique
            const allArgs = [...new Set([...rawPaths, ...vol1Paths])];

            // Use du -skc 
            const cmd = `du -skc ${allArgs.map(escapeShellArg).join(' ')}`;
            // console.log('DEBUG: Executing du command:', cmd); 

            conn.exec(cmd, (err, stream) => {
                if (err) {
                    conn.end();
                    return reject(err);
                }

                let output = '';
                stream.on('close', (code, signal) => {
                    conn.end();
                    // console.log('DEBUG: Raw du output:', JSON.stringify(output)); 

                    const sizes = {};
                    output.split('\n').forEach(line => {
                        const parts = line.trim().split(/\s+/);
                        // Output format: Size Path
                        if (parts.length >= 2) {
                            const size = parseInt(parts[0], 10); // Size in KB
                            const pathStr = parts.slice(1).join(' ');

                            if (pathStr !== 'total' && !isNaN(size)) {
                                const bytes = size * 1024;

                                // Direct match?
                                if (paths.includes(pathStr)) {
                                    sizes[pathStr] = bytes;
                                }
                                // /volume1 match?
                                else if (pathStr.startsWith('/volume1')) {
                                    const stripped = pathStr.substring(8); // remove /volume1
                                    if (paths.includes(stripped)) {
                                        sizes[stripped] = bytes;
                                    }
                                }
                            }
                        }
                    });
                    resolve(sizes);
                }).on('data', (data) => {
                    output += data;
                }).stderr.on('data', (data) => {
                    // Suppress "cannot access" errors since we expect them for the wrong paths
                    // console.error('DEBUG: DU STDERR:', data.toString());
                });
            });
        } catch (err) {
            if (conn) conn.end();
            reject(err);
        }
    });
}

module.exports = { connect, listRemote, getConnectionConfig, getFolderSizes };
