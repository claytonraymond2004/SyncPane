# Selective Sync Web App

A modern web application to sync folders from a remote server to your local file system via SSH/SFTP. Features a premium UI, job queuing, scheduling, and connection resiliency.

## Features

- **Remote Browser**: Browse your remote server via SSH.
- **Selective Sync**: Choose specific files or folders to sync.
- **Job Queue**: Monitor sync progress, reorder priority, and view history.
- **Resiliency**: Auto-retries, connection checks, and clean error handling.
- **Scheduling**: Automatic hourly sync checks or custom Cron expressions.
- **Global Settings**: Master sync toggle, custom frequency, and favorite paths management.
- **Configuration**: securely store credentials (encrypted).


## Quick Start (Docker)

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/claytonraymond2004/selective-sync.git
    cd selective-sync
    ```

2.  **Configure Environment:**
    Copy the sample environment file:
    ```bash
    cp .env.sample .env
    ```
    Open `.env` and set your `ENCRYPTION_KEY` (generate one with `openssl rand -hex 32`).


3.  **Configure Volumes (Optional):**
    Open `compose.yml` to set your sync directories.
    > [!NOTE]
    > It is recommended to mount local directories under the `/app/` path inside the container (e.g., `./sync:/app/sync`), as the application's file explorer defaults to this location.

4.  **Run with Docker Compose:**
    ```bash
    docker-compose up -d
    ```


5.  **Open**: [http://localhost:3001](http://localhost:3001)

## Development

### Prerequisites
- Node.js 18+
- SQLite3

### Setup

1. **Install Dependencies**:
   ```bash
   cd server && npm install
   cd ../client && npm install
   ```

2. **Run Backend** (Port 3001):
   ```bash
   cd server
   # Create a .env file with ENCRYPTION_KEY=...
   npm run dev
   ```

3. **Run Frontend** (Port 5173 - proxies to 3001):
   ```bash
   cd client
   npm run dev
   ```


### Development with Docker (Hot Reload)

To run the full stack in development mode with hot reloading (no rebuilds required):

```bash
docker-compose -f docker-compose.dev.yml up
```


- **Frontend**: [http://localhost:3001](http://localhost:3001)
- **Backend API**: [http://localhost:3000](http://localhost:3000)

Changes to files in `server/` or `client/` will automatically trigger updates.


## Building Locally

If you prefer to build the production image locally instead of pulling from the registry, use the build-specific compose file:


```bash
docker-compose -f docker-compose.build.yml up -d --build
```

**Open**: [http://localhost:3001](http://localhost:3001)


## Configuration

- **Encryption**: Credentials are encrypted using AES-256-CBC with the key provided in `ENCRYPTION_KEY`.
- **Sync Logic**: Remote files always take precedence. If a file changes remotely, it is re-downloaded. Local deletions are respected (sync stops if local target is missing) unless re-enabled.


## Tech Stack


- **Frontend**: React, Vite, Vanilla CSS (Premium Design).
- **Backend**: Node.js, Express, Better-SQLite3, SSH2.
