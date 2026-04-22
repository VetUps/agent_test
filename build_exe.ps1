$ErrorActionPreference = "Stop"

$python = "python"

& $python -m PyInstaller `
  --noconfirm `
  --clean `
  --onedir `
  --name analytics_agent `
  --collect-submodules dash `
  --collect-submodules plotly `
  --collect-submodules pandas `
  --collect-submodules numpy `
  run_agent.py
