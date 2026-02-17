const { app, BrowserWindow, dialog, Notification } = require('electron');
const { autoUpdater } = require('electron-updater');
const { spawn } = require('child_process');
const fs = require('fs');
const http = require('http');
const net = require('net');
const path = require('path');

const BACKEND_HOST = '127.0.0.1';
const PORT_BASE = 18740;
const PORT_RANGE = 20;
const HEALTH_PATH = '/api/health';
const HEALTH_TIMEOUT_MS = 45000;
const HEALTH_INTERVAL_MS = 450;
const SHUTDOWN_TIMEOUT_MS = 5000;

let mainWindow = null;
let backendProcess = null;
let backendPort = null;
let isQuitting = false;
let updateCheckRequested = false;
let shutdownInProgress = false;

function runtimeDataDir() {
    const dir = path.join(app.getPath('userData'), 'runtime');
    fs.mkdirSync(dir, { recursive: true });
    return dir;
}

function runtimeLogPath() {
    return path.join(runtimeDataDir(), 'desktop.log');
}

function log(message, extra = null) {
    const line = `[${new Date().toISOString()}] ${message}${extra ? ` ${JSON.stringify(extra)}` : ''}\n`;
    try {
        fs.appendFileSync(runtimeLogPath(), line, 'utf8');
    } catch (_e) {
        // best-effort logging only
    }
}

function notify(title, body) {
    if (!Notification.isSupported()) return;
    try {
        const notification = new Notification({ title, body });
        notification.show();
    } catch (_e) {
        // best-effort notification only
    }
}

function checkPortAvailable(port) {
    return new Promise(resolve => {
        const server = net.createServer();
        server.once('error', () => resolve(false));
        server.once('listening', () => server.close(() => resolve(true)));
        server.listen(port, BACKEND_HOST);
    });
}

async function chooseBackendPort() {
    for (let offset = 0; offset < PORT_RANGE; offset += 1) {
        const candidate = PORT_BASE + offset;
        if (await checkPortAvailable(candidate)) return candidate;
    }
    throw new Error(`No available backend ports found in range ${PORT_BASE}-${PORT_BASE + PORT_RANGE - 1}`);
}

function readJsonResponse(url) {
    return new Promise((resolve, reject) => {
        const req = http.get(url, { timeout: 2200 }, (res) => {
            let body = '';
            res.on('data', chunk => {
                body += chunk;
            });
            res.on('end', () => {
                if (res.statusCode !== 200) {
                    reject(new Error(`Health check failed with status ${res.statusCode}`));
                    return;
                }
                try {
                    resolve(JSON.parse(body || '{}'));
                } catch (error) {
                    reject(error);
                }
            });
        });

        req.on('error', reject);
        req.on('timeout', () => req.destroy(new Error('Health check timeout')));
    });
}

async function waitForBackendHealth(port, timeoutMs = HEALTH_TIMEOUT_MS) {
    const start = Date.now();
    const url = `http://${BACKEND_HOST}:${port}${HEALTH_PATH}`;

    while ((Date.now() - start) < timeoutMs) {
        try {
            const payload = await readJsonResponse(url);
            if (payload?.ok) return payload;
        } catch (_err) {
            // continue polling
        }
        await new Promise(resolve => setTimeout(resolve, HEALTH_INTERVAL_MS));
    }
    throw new Error(`Timed out waiting for backend health endpoint at ${url}`);
}

function resolvePackagedSidecarPath() {
    const executableName = process.platform === 'win32' ? 'hound-backend.exe' : 'hound-backend';
    const candidates = [
        path.join(process.resourcesPath, 'sidecar', 'hound-backend', executableName),
        path.join(process.resourcesPath, 'sidecar', executableName),
    ];
    for (const candidate of candidates) {
        if (fs.existsSync(candidate)) return candidate;
    }
    throw new Error(`Unable to locate packaged sidecar executable. Tried: ${candidates.join(', ')}`);
}

function startBackendProcess(port) {
    const dataDir = runtimeDataDir();
    const sharedEnv = {
        ...process.env,
        HOUND_DATA_DIR: dataDir,
        HOUND_PORT: String(port),
        HOUND_HOST: BACKEND_HOST,
    };
    const args = ['--host', BACKEND_HOST, '--port', String(port), '--data-dir', dataDir, '--log-level', 'info'];

    if (app.isPackaged) {
        const sidecarExec = resolvePackagedSidecarPath();
        log('Starting packaged backend sidecar', { sidecarExec, port, dataDir });
        return spawn(sidecarExec, args, {
            env: sharedEnv,
            windowsHide: true,
            stdio: ['ignore', 'pipe', 'pipe'],
        });
    }

    const projectRoot = path.resolve(__dirname, '..');
    const serverPath = path.join(projectRoot, 'server.py');
    const pythonCmd = process.platform === 'win32' ? 'python' : 'python3';
    log('Starting development backend via Python', { pythonCmd, serverPath, port, dataDir });
    return spawn(pythonCmd, [serverPath, ...args], {
        cwd: projectRoot,
        env: sharedEnv,
        windowsHide: true,
        stdio: ['ignore', 'pipe', 'pipe'],
    });
}

function attachBackendLogging(proc) {
    if (!proc) return;
    proc.stdout?.on('data', (chunk) => log('backend:stdout', { line: String(chunk || '').trim() }));
    proc.stderr?.on('data', (chunk) => log('backend:stderr', { line: String(chunk || '').trim() }));
    proc.on('error', (error) => log('backend:process-error', { message: error?.message || String(error) }));
    proc.on('close', (code, signal) => {
        log('backend:process-closed', { code, signal, quitting: isQuitting });
        backendProcess = null;
        if (!isQuitting) {
            dialog.showErrorBox(
                'Backend Stopped',
                'The Hound backend process exited unexpectedly. Please relaunch the app.'
            );
            app.quit();
        }
    });
}

async function bootBackend() {
    backendPort = await chooseBackendPort();
    backendProcess = startBackendProcess(backendPort);
    attachBackendLogging(backendProcess);
    await waitForBackendHealth(backendPort);
    log('backend:health-ok', { port: backendPort });
}

async function shutdownBackend() {
    if (!backendProcess) return;
    const proc = backendProcess;
    backendProcess = null;
    try {
        proc.kill();
    } catch (_e) {
        return;
    }

    await new Promise(resolve => {
        const timer = setTimeout(() => {
            try {
                proc.kill('SIGKILL');
            } catch (_e) {
                // no-op
            }
            resolve();
        }, SHUTDOWN_TIMEOUT_MS);

        proc.once('close', () => {
            clearTimeout(timer);
            resolve();
        });
    });
}

function createMainWindow() {
    if (!backendPort) throw new Error('Backend port is unavailable.');

    mainWindow = new BrowserWindow({
        width: 1460,
        height: 920,
        minWidth: 1120,
        minHeight: 760,
        show: false,
        autoHideMenuBar: true,
        webPreferences: {
            contextIsolation: true,
            nodeIntegration: false,
            preload: path.join(__dirname, 'preload.js'),
        },
    });

    const appUrl = `http://${BACKEND_HOST}:${backendPort}/static/index.html`;
    log('window:load-url', { appUrl });
    mainWindow.loadURL(appUrl);
    mainWindow.once('ready-to-show', () => mainWindow?.show());
    mainWindow.on('closed', () => {
        mainWindow = null;
    });
}

function configureUpdaterFeed() {
    const owner = String(process.env.HOUND_UPDATE_OWNER || process.env.GH_REPO_OWNER || '').trim();
    const repo = String(process.env.HOUND_UPDATE_REPO || process.env.GH_REPO_NAME || '').trim();
    if (!owner || !repo) return;

    autoUpdater.setFeedURL({
        provider: 'github',
        owner,
        repo,
        private: false,
    });
}

function wireAutoUpdater() {
    if (!app.isPackaged) {
        log('updater:skipped-dev-mode');
        return;
    }

    configureUpdaterFeed();
    autoUpdater.autoDownload = false;
    autoUpdater.autoInstallOnAppQuit = true;

    autoUpdater.on('checking-for-update', () => {
        log('updater:checking');
        notify('Hound Suite', 'Checking for updates...');
    });

    autoUpdater.on('update-available', async (info) => {
        log('updater:available', { version: info?.version });
        const result = await dialog.showMessageBox({
            type: 'info',
            buttons: ['Update now', 'Skip'],
            defaultId: 0,
            cancelId: 1,
            title: 'Update Available',
            message: `Hound Suite ${info?.version || 'latest'} is available.`,
            detail: 'An update is available. You will see this reminder on each launch until you update.',
        });
        if (result.response === 0) {
            updateCheckRequested = true;
            notify('Hound Suite', 'Downloading update...');
            autoUpdater.downloadUpdate().catch((error) => {
                log('updater:download-failed', { message: error?.message || String(error) });
                dialog.showErrorBox('Update Failed', 'Unable to download the update. Please try again later.');
            });
        }
    });

    autoUpdater.on('update-not-available', (info) => {
        log('updater:not-available', { version: info?.version });
    });

    autoUpdater.on('update-downloaded', async (info) => {
        log('updater:downloaded', { version: info?.version });
        notify('Hound Suite', 'Update ready to install.');
        const result = await dialog.showMessageBox({
            type: 'question',
            buttons: ['Restart and Install', 'Later'],
            defaultId: 0,
            cancelId: 1,
            title: 'Install Update',
            message: `Hound Suite ${info?.version || 'update'} has been downloaded.`,
            detail: 'Restart now to apply the update.',
        });
        if (result.response === 0) {
            isQuitting = true;
            autoUpdater.quitAndInstall(false, true);
        }
    });

    autoUpdater.on('error', (error) => {
        log('updater:error', { message: error?.message || String(error) });
        if (updateCheckRequested) {
            dialog.showErrorBox('Update Error', 'There was a problem applying updates. The current version will keep running.');
        }
    });

    autoUpdater.checkForUpdates().catch((error) => {
        log('updater:check-failed', { message: error?.message || String(error) });
    });
}

async function bootstrap() {
    try {
        await bootBackend();
        createMainWindow();
        wireAutoUpdater();
    } catch (error) {
        log('bootstrap:failed', { message: error?.message || String(error) });
        dialog.showErrorBox(
            'Unable to Launch Hound Suite',
            `Startup failed:\n${error?.message || String(error)}`
        );
        await shutdownBackend();
        app.quit();
    }
}

app.on('before-quit', (event) => {
    if (shutdownInProgress) return;
    if (!backendProcess) {
        isQuitting = true;
        return;
    }
    event.preventDefault();
    shutdownInProgress = true;
    isQuitting = true;
    shutdownBackend().finally(() => {
        app.exit(0);
    });
});

app.on('window-all-closed', () => {
    app.quit();
});

app.on('activate', () => {
    if (!mainWindow && backendPort) createMainWindow();
});

app.whenReady().then(() => {
    log('app:ready', { packaged: app.isPackaged, version: app.getVersion() });
    bootstrap();
});
