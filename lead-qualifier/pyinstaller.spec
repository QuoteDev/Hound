# -*- mode: python ; coding: utf-8 -*-

from pathlib import Path


PROJECT_DIR = Path(globals().get("SPEC", Path.cwd())).resolve().parent

datas = [
    (str(PROJECT_DIR / "static"), "static"),
    (str(PROJECT_DIR / "resources"), "resources"),
]

geoip_db = PROJECT_DIR / "GeoLite2-Country.mmdb"
if geoip_db.exists():
    datas.append((str(geoip_db), "."))

hiddenimports = [
    "uvicorn",
    "uvicorn.logging",
    "uvicorn.loops.auto",
    "uvicorn.protocols.http.auto",
    "uvicorn.protocols.websockets.auto",
    "uvicorn.lifespan.on",
]

a = Analysis(
    [str(PROJECT_DIR / "desktop" / "sidecar_main.py")],
    pathex=[str(PROJECT_DIR)],
    binaries=[],
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name="hound-backend",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=False,
)

coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name="hound-backend",
)
