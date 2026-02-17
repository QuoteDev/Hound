# Hound Suite Desktop Distribution

This project now supports production desktop packaging for macOS and Windows with launch-time auto-update checks.

## Local Packaging Commands

From `/Users/gavinwinchell/Documents/Hound/lead-qualifier`:

```bash
export GH_REPO_OWNER=<your-github-owner-or-org>
export GH_REPO_NAME=<your-github-repo>
source .venv/bin/activate
npm install
python3 -m pip install -r requirements.txt pyinstaller
npm run package:mac
```

To build Windows installer from a Windows machine:

```bash
npm run package:win
```

PowerShell env setup for Windows before packaging:

```powershell
$env:GH_REPO_OWNER="<your-github-owner-or-org>"
$env:GH_REPO_NAME="<your-github-repo>"
```

To build both on one machine where supported:

```bash
npm run package:all
```

## Build Outputs

- macOS:
  - `.dmg` installer
  - update metadata (`latest-mac.yml` + zip/blockmap artifacts)
- Windows:
  - `.exe` NSIS installer
  - update metadata (`latest.yml` + blockmap artifacts)

Artifacts are generated into `release/` during CI.

## Auto-Update Behavior

- On every launch, the app checks GitHub Releases for a newer version.
- If an update exists, users see:
  - `Update now`
  - `Skip`
- `Skip` only skips that launch. The prompt returns on next launch until the app is updated.

## Coworker Install Notes (Unsigned Builds)

### macOS

Because this build is unsigned:

1. Open the `.dmg` and drag **Hound Suite** into Applications.
2. First launch:
   - Right-click the app in Applications
   - Select **Open**
   - Confirm **Open** in the security prompt

### Windows

Because this build is unsigned:

1. Run the `.exe` installer.
2. If SmartScreen appears:
   - Click **More info**
   - Click **Run anyway**

## Data Persistence

Desktop runtime data is stored in the app user-data directory under:

- `runtime/` (logs + backend data)
- backend data includes:
  - `domain_cache.db`
  - `session_store/`

This path is version-safe and survives app updates.
