#!/usr/bin/env pwsh
# Hermes Weave Plugin 安装脚本 - Windows PowerShell
# 用法: irm https://raw.githubusercontent.com/Weave-chat/hermes-weave-plugin/main/install.ps1 | iex
#   或: git clone https://github.com/Weave-chat/hermes-weave-plugin && powershell -ExecutionPolicy Bypass -File install.ps1
$ErrorActionPreference = "Stop"

# irm | iex 模式下脚本结束或出错会直接关闭窗口，用 trap 捕获意外错误并暂停
trap {
    Write-Host ""
    Write-Host "[x] 错误: $_" -ForegroundColor Red
    Write-Host ""
    Read-Host "按 Enter 键退出"
    exit 1
}

# ── 辅助函数 ──
function Write-Info  { param([string]$msg) Write-Host "[i] $msg" -ForegroundColor Cyan }
function Write-Ok    { param([string]$msg) Write-Host "[v] $msg" -ForegroundColor Green }
function Write-Warn  { param([string]$msg) Write-Host "[!] $msg" -ForegroundColor Yellow }
function Write-Fail  {
    param([string]$msg)
    Write-Host "[x] $msg" -ForegroundColor Red
    Write-Host ""
    Read-Host "按 Enter 键退出"
    exit 1
}

# UTF-8 无 BOM 编码（写 .env 和 config.yaml 时使用，避免 Python 读取时 BOM 导致 key 解析错误）
$utf8NoBom = New-Object System.Text.UTF8Encoding $false

Write-Host ""
Write-Host "==========================================" -ForegroundColor White
Write-Host "   Hermes Weave Plugin 安装程序          " -ForegroundColor White
Write-Host "==========================================" -ForegroundColor White
Write-Host ""

# ═══════════════════════════════════════════
# [1/6] 环境检查
# ═══════════════════════════════════════════
Write-Info "[1/6] 环境检查..."

# Python（Windows 上优先 python，然后 py launcher）
$pythonCmd = $null
foreach ($cmd in @("python", "python3", "py")) {
    if (Get-Command $cmd -ErrorAction SilentlyContinue) {
        $pythonCmd = $cmd
        break
    }
}
if (-not $pythonCmd) {
    Write-Fail "未找到 Python，请先安装 Python 3.10+"
}
$pythonVersion = & $pythonCmd --version 2>&1
Write-Ok "Python: $pythonVersion"

# 检测用户目录（Windows 用 USERPROFILE，不会被 Hermes profile 覆盖）
$realHome = $env:USERPROFILE
if (-not $realHome) {
    $realHome = & $pythonCmd -c "import os; print(os.path.expanduser('~'))" 2>$null
}
if (-not $realHome) {
    Write-Fail "无法确定用户目录"
}
$hermesHome = Join-Path $realHome ".hermes"

# Hermes CLI
$hermes = $null
if (Get-Command hermes -ErrorAction SilentlyContinue) {
    $hermes = "hermes"
} else {
    foreach ($p in @("$realHome\.local\bin\hermes.exe", "$realHome\.local\bin\hermes.cmd")) {
        if (Test-Path $p) {
            $hermes = $p
            break
        }
    }
}
if ($hermes) {
    Write-Ok "Hermes CLI: $hermes"
} else {
    Write-Warn "未找到 hermes CLI（插件仍可安装，但需要手动启用）"
}

# Hermes home 目录
if (-not (Test-Path $hermesHome)) {
    Write-Fail "Hermes 目录不存在: $hermesHome（请先安装 Hermes Agent）"
}
Write-Ok "Hermes Home: $hermesHome"

# ═══════════════════════════════════════════
# [2/6] 选择 Profile
# ═══════════════════════════════════════════
Write-Info "[2/6] 选择 Profile..."

$profilesDir = Join-Path $hermesHome "profiles"
if (-not (Test-Path $profilesDir)) {
    Write-Fail "Profile 目录不存在: $profilesDir"
}

# 收集所有 profile（排除 backups 等非 profile 目录）
$profiles = @()
Get-ChildItem $profilesDir -Directory -ErrorAction SilentlyContinue | ForEach-Object {
    $name = $_.Name
    if ($name -eq "backups") { return }
    $hasConfig = Test-Path (Join-Path $_.FullName "config.yaml")
    $hasEnv = Test-Path (Join-Path $_.FullName ".env")
    if ($hasConfig -or $hasEnv) {
        $profiles += $name
    }
}

if ($profiles.Count -eq 0) {
    Write-Fail "未找到任何 Profile（请先创建: hermes -p <name> init）"
}

Write-Host ""
Write-Host "  可用的 Profile:"
for ($i = 0; $i -lt $profiles.Count; $i++) {
    Write-Host "  $($i+1). $($profiles[$i])"
}
Write-Host ""

# 选择 profile
while ($true) {
    $choice = Read-Host "  请选择 Profile 序号 [1-$($profiles.Count)]"
    if ($choice -match '^\d+$' -and [int]$choice -ge 1 -and [int]$choice -le $profiles.Count) {
        $selectedProfile = $profiles[[int]$choice - 1]
        break
    }
    Write-Warn "无效输入，请输入 1-$($profiles.Count) 之间的数字"
}

$profileDir = Join-Path $profilesDir $selectedProfile
$pluginDir = Join-Path $profileDir "plugins\weave-platform"
$envFile = Join-Path $profileDir ".env"
$configFile = Join-Path $profileDir "config.yaml"

Write-Ok "Profile: $selectedProfile"
Write-Ok "安装路径: $pluginDir"
Write-Host ""

# 确认
$confirm = Read-Host "  确认安装到 $selectedProfile? [Y/n]"
if ($confirm -match '^[Nn]') {
    Write-Host "已取消。"
    exit 0
}
Write-Host ""

# ═══════════════════════════════════════════
# [3/6] 下载插件
# ═══════════════════════════════════════════
Write-Info "[3/6] 下载插件..."

$repoUrl = "https://github.com/Weave-chat/hermes-weave-plugin"
$tmpDir = Join-Path $env:TEMP "weave_install_$(Get-Random)"
New-Item -ItemType Directory -Path $tmpDir -Force | Out-Null

try {
    $clonePath = Join-Path $tmpDir "weave"

    if (Get-Command git -ErrorAction SilentlyContinue) {
        & git clone --depth 1 $repoUrl $clonePath
        if ($LASTEXITCODE -ne 0) {
            Write-Fail "git clone 失败（请检查网络连接或仓库地址: $repoUrl）"
        }
        Write-Ok "git clone 完成"
    } else {
        Write-Warn "未找到 git，使用 PowerShell 下载..."
        $zipUrl = "$repoUrl/archive/refs/heads/main.zip"
        $zipPath = Join-Path $tmpDir "weave.zip"
        try {
            Invoke-WebRequest -Uri $zipUrl -OutFile $zipPath -UseBasicParsing
            Expand-Archive -Path $zipPath -DestinationPath $tmpDir -Force
            Move-Item (Join-Path $tmpDir "hermes-weave-plugin-main") $clonePath
            Write-Ok "下载完成"
        } catch {
            Write-Fail "下载失败: $_"
        }
    }

    # 检查下载内容
    if (-not (Test-Path (Join-Path $clonePath "plugin.yaml"))) {
        Write-Fail "下载的文件不完整（缺少 plugin.yaml）"
    }
    if (-not (Test-Path (Join-Path $clonePath "adapter.py"))) {
        Write-Fail "下载的文件不完整（缺少 adapter.py）"
    }

    # 安装到目标路径
    $pluginsParent = Split-Path $pluginDir -Parent
    if (-not (Test-Path $pluginsParent)) {
        New-Item -ItemType Directory -Path $pluginsParent -Force | Out-Null
    }
    if (Test-Path $pluginDir) {
        Remove-Item $pluginDir -Recurse -Force
    }
    Copy-Item -Path $clonePath -Destination $pluginDir -Recurse
    Write-Ok "已安装到 $pluginDir"
} finally {
    if (Test-Path $tmpDir) {
        Remove-Item $tmpDir -Recurse -Force -ErrorAction SilentlyContinue
    }
}

# ═══════════════════════════════════════════
# [4/6] 配置环境变量
# ═══════════════════════════════════════════
Write-Info "[4/6] 配置环境变量..."

# 确保配置文件存在
if (-not (Test-Path $envFile)) {
    [System.IO.File]::WriteAllText($envFile, "", $utf8NoBom)
}

# 读取已有值（不覆盖）
function Get-EnvValue {
    param([string]$key)
    if (Test-Path $envFile) {
        $lines = Get-Content $envFile -ErrorAction SilentlyContinue
        foreach ($line in $lines) {
            if ($line -match "^$key=(.*)$") {
                return $matches[1]
            }
        }
    }
    return $null
}

# 写入环境变量（如果不存在）
function Set-EnvIfAbsent {
    param([string]$key, [string]$val)
    $existing = Get-EnvValue $key
    if ($null -eq $existing) {
        [System.IO.File]::AppendAllText($envFile, "$key=$val`n", $utf8NoBom)
        Write-Ok "已写入 $key"
    } else {
        $preview = $existing
        if ($existing.Length -gt 20) { $preview = $existing.Substring(0, 20) }
        Write-Ok "$key 已存在: ${preview}...（保留）"
    }
}

Write-Host ""
Write-Host "  请输入 Weave 连接配置:"
Write-Host ""

# WEAVE_WS_URL
$existingUrl = Get-EnvValue "WEAVE_WS_URL"
if ($existingUrl) {
    Write-Ok "WEAVE_WS_URL 已存在: $existingUrl（保留）"
} else {
    $inputUrl = Read-Host "  Weave WebSocket URL [wss://www.weaveai.chat]"
    if (-not $inputUrl) { $inputUrl = "wss://www.weaveai.chat" }
    Set-EnvIfAbsent "WEAVE_WS_URL" $inputUrl
}

# WEAVE_WS_ID
$existingId = Get-EnvValue "WEAVE_WS_ID"
if ($existingId) {
    $preview = $existingId
    if ($existingId.Length -gt 12) { $preview = $existingId.Substring(0, 12) }
    Write-Ok "WEAVE_WS_ID 已存在: ${preview}...（保留）"
} else {
    Write-Host ""
    Write-Host "  WS ID 是在 Weave 中创建 AI 联系人时生成的标识符。"
    Write-Host "  获取方式: Weave 网站 -> 联系人菜单 -> 添加 AI 联系人 -> 复制 WS ID"
    Write-Host ""
    while ($true) {
        $inputId = Read-Host "  Weave WS ID"
        if ($inputId) {
            Set-EnvIfAbsent "WEAVE_WS_ID" $inputId
            break
        }
        Write-Warn "WS ID 不能为空"
    }
}

# WEAVE_API_KEY（可选）
$existingKey = Get-EnvValue "WEAVE_API_KEY"
if ($existingKey) {
    Write-Ok "WEAVE_API_KEY 已存在（保留）"
} else {
    $inputKey = Read-Host "  API Key（可选，直接回车跳过）"
    if ($inputKey) {
        Set-EnvIfAbsent "WEAVE_API_KEY" $inputKey
    } else {
        Write-Info "跳过 API Key（演示模式，无需认证）"
    }
}

Write-Host ""

# ═══════════════════════════════════════════
# [5/6] 启用插件
# ═══════════════════════════════════════════
Write-Info "[5/6] 启用插件..."

$enablePluginManually = $false

# 方式一：优先用 hermes CLI
if ($hermes) {
    $enabled = & $hermes -p $selectedProfile plugins list 2>$null | Select-String "weave-platform" | Select-Object -First 1
    if ($enabled -match "enabled") {
        Write-Ok "weave-platform 已启用"
    } elseif ($enabled) {
        # 已安装但未启用
        & $hermes -p $selectedProfile plugins enable weave-platform 2>$null
        if ($LASTEXITCODE -eq 0) {
            Write-Ok "已通过 CLI 启用"
        } else {
            Write-Warn "CLI 启用失败，尝试手动修改 config.yaml..."
            $enablePluginManually = $true
        }
    } else {
        # 未安装到 hermes 的插件系统，手动修改 config.yaml
        $enablePluginManually = $true
    }
} else {
    $enablePluginManually = $true
}

# 方式二：手动修改 config.yaml
if ($enablePluginManually) {
    if (-not (Test-Path $configFile)) {
        # config.yaml 不存在，创建
        $yamlContent = @"
plugins:
  enabled:
  - weave-platform
  disabled: []
"@
        [System.IO.File]::WriteAllText($configFile, $yamlContent, $utf8NoBom)
        Write-Ok "已创建 config.yaml 并启用 weave-platform"
    } else {
        # 检查是否已启用
        $content = Get-Content $configFile -Raw -ErrorAction SilentlyContinue
        if ($content -match "weave-platform") {
            Write-Ok "config.yaml 中已有 weave-platform"
        } else {
            # 用 python 安全修改 YAML
            $pyScript = @"
import yaml
config_path = r'$configFile'
try:
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f) or {}
except Exception:
    config = {}
if 'plugins' not in config:
    config['plugins'] = {'enabled': [], 'disabled': []}
if 'enabled' not in config['plugins']:
    config['plugins']['enabled'] = []
if 'disabled' not in config['plugins']:
    config['plugins']['disabled'] = []
if 'weave-platform' not in config['plugins']['enabled']:
    config['plugins']['enabled'].append('weave-platform')
    if 'weave-platform' in config['plugins'].get('disabled', []):
        config['plugins']['disabled'].remove('weave-platform')
with open(config_path, 'w') as f:
    yaml.dump(config, f, default_flow_style=False, allow_unicode=True)
print('OK')
"@
            $result = & $pythonCmd -c $pyScript 2>&1
            if ($result -match "OK") {
                Write-Ok "已在 config.yaml 中启用 weave-platform"
            } else {
                Write-Warn "自动修改 config.yaml 失败，请手动添加 weave-platform 到 plugins.enabled"
                Write-Warn "$result"
            }
        }
    }
}

# ═══════════════════════════════════════════
# [6/6] 完成
# ═══════════════════════════════════════════
Write-Host ""
Write-Host "==========================================" -ForegroundColor Green
Write-Host "          安装完成！                      " -ForegroundColor Green
Write-Host "==========================================" -ForegroundColor Green
Write-Host ""
Write-Host "  配置摘要:"
Write-Host "    Profile:    $selectedProfile"
Write-Host "    插件路径:   $pluginDir"
Write-Host "    环境变量:   $envFile"
Write-Host "    WS URL:     $(Get-EnvValue 'WEAVE_WS_URL')"
$wsId = Get-EnvValue 'WEAVE_WS_ID'
if ($wsId) {
    $wsIdPreview = $wsId
    if ($wsId.Length -gt 12) { $wsIdPreview = $wsId.Substring(0, 12) }
    Write-Host "    WS ID:      ${wsIdPreview}..."
}
Write-Host ""
Write-Host "  下一步: 重启网关使插件生效"
Write-Host ""
Write-Host "    hermes -p $selectedProfile gateway run --replace"
Write-Host ""
Write-Host "  详细文档: https://www.weaveai.chat/docs/hermes"
Write-Host ""
Write-Host ""
Read-Host "按 Enter 键退出"
