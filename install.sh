#!/bin/bash
# Hermes Weave Plugin 安装脚本 - macOS / Linux
# 用法: curl -sSL https://raw.githubusercontent.com/Weave-chat/hermes-weave-plugin/main/install.sh | bash
#   或: git clone https://github.com/Weave-chat/hermes-weave-plugin && bash install.sh
set -e

REPO="https://github.com/Weave-chat/hermes-weave-plugin"
# HERMES_HOME 始终指向 ~/.hermes 根目录（不使用环境变量，因为 profile 下会覆盖）
HERMES_HOME=""

# ── 颜色 ──
if [ -t 1 ]; then
    CYAN='\033[0;36m'; GREEN='\033[0;32m'; YELLOW='\033[0;33m'
    RED='\033[0;31m'; BOLD='\033[1m'; NC='\033[0m'
else
    CYAN=''; GREEN=''; YELLOW=''; RED=''; BOLD=''; NC=''
fi

info()  { echo -e "${CYAN}ℹ${NC}  $1"; }
ok()    { echo -e "${GREEN}✓${NC}  $1"; }
warn()  { echo -e "${YELLOW}⚠${NC}  $1"; }
fail()  { echo -e "${RED}✗${NC}  $1"; exit 1; }

echo ""
echo -e "${BOLD}╔══════════════════════════════════════════╗${NC}"
echo -e "${BOLD}║   Hermes Weave Plugin 安装程序          ║${NC}"
echo -e "${BOLD}╚══════════════════════════════════════════╝${NC}"
echo ""

# ═══════════════════════════════════════════
# [1/6] 环境检查
# ═══════════════════════════════════════════
info "[1/6] 环境检查..."

# Python
if command -v python3 &>/dev/null; then
    PYTHON="python3"
elif command -v python &>/dev/null; then
    PYTHON="python"
else
    fail "未找到 Python，请先安装 Python 3.10+"
fi
ok "Python: $($PYTHON --version 2>&1)"

# 检测真实用户目录（Hermes profile 可能覆盖 $HOME）
REAL_HOME=$($PYTHON -c "import os, pwd; print(pwd.getpwuid(os.getuid()).pw_dir)" 2>/dev/null || echo "$HOME")
HERMES_HOME="$REAL_HOME/.hermes"

# Hermes CLI
HERMES=""
if command -v hermes &>/dev/null; then
    HERMES="hermes"
elif [ -x "$HOME/.local/bin/hermes" ]; then
    HERMES="$HOME/.local/bin/hermes"
fi
if [ -n "$HERMES" ]; then
    ok "Hermes CLI: $HERMES"
else
    warn "未找到 hermes CLI（插件仍可安装，但需要手动启用）"
fi

# Hermes home 目录
if [ ! -d "$HERMES_HOME" ]; then
    fail "Hermes 目录不存在: $HERMES_HOME（请先安装 Hermes Agent）"
fi
ok "Hermes Home: $HERMES_HOME"

# ═══════════════════════════════════════════
# [2/6] 选择 Profile
# ═══════════════════════════════════════════
info "[2/6] 选择 Profile..."

PROFILES_DIR="$HERMES_HOME/profiles"
if [ ! -d "$PROFILES_DIR" ]; then
    fail "Profile 目录不存在: $PROFILES_DIR"
fi

# 收集所有 profile（排除 backups 等非 profile 目录）
PROFILES=()
for d in "$PROFILES_DIR"/*/; do
    name=$(basename "$d")
    # 跳过明显不是 profile 的目录
    [ "$name" = "backups" ] && continue
    [ -f "${d}config.yaml" ] || [ -f "${d}.env" ] || continue
    PROFILES+=("$name")
done

if [ ${#PROFILES[@]} -eq 0 ]; then
    fail "未找到任何 Profile（请先创建: hermes -p <name> init）"
fi

echo ""
echo "  可用的 Profile:"
for i in "${!PROFILES[@]}"; do
    echo "  $((i+1)). ${PROFILES[$i]}"
done
echo ""

# 选择 profile
while true; do
    read -p "  请选择 Profile 序号 [1-${#PROFILES[@]}]: " choice
    if [[ "$choice" =~ ^[0-9]+$ ]] && [ "$choice" -ge 1 ] && [ "$choice" -le ${#PROFILES[@]} ]; then
        SELECTED_PROFILE="${PROFILES[$((choice-1))]}"
        break
    fi
    warn "无效输入，请输入 1-${#PROFILES[@]} 之间的数字"
done

PROFILE_DIR="$PROFILES_DIR/$SELECTED_PROFILE"
PLUGIN_DIR="$PROFILE_DIR/plugins/weave-platform"
ENV_FILE="$PROFILE_DIR/.env"
CONFIG_FILE="$PROFILE_DIR/config.yaml"

ok "Profile: $SELECTED_PROFILE"
ok "安装路径: $PLUGIN_DIR"
echo ""

# 确认
read -p "  确认安装到 $SELECTED_PROFILE? [Y/n]: " confirm
[[ "$confirm" =~ ^[Nn] ]] && { echo "已取消。"; exit 0; }
echo ""

# ═══════════════════════════════════════════
# [3/6] 下载插件
# ═══════════════════════════════════════════
info "[3/6] 下载插件..."

TMP_DIR=$(mktemp -d)
trap 'rm -rf "$TMP_DIR"' EXIT

if command -v git &>/dev/null; then
    git clone --depth 1 "$REPO" "$TMP_DIR/weave" 2>/dev/null
    ok "git clone 完成"
else
    warn "未找到 git，使用 curl 下载..."
    if command -v curl &>/dev/null; then
        curl -sSL "$REPO/archive/refs/heads/main.tar.gz" | tar xz -C "$TMP_DIR"
        mv "$TMP_DIR/hermes-weave-plugin-main" "$TMP_DIR/weave"
        ok "下载完成"
    else
        fail "未找到 git 和 curl，无法下载"
    fi
fi

# 检查下载内容
[ -f "$TMP_DIR/weave/plugin.yaml" ] || fail "下载的文件不完整（缺少 plugin.yaml）"
[ -f "$TMP_DIR/weave/adapter.py" ] || fail "下载的文件不完整（缺少 adapter.py）"

# 安装到目标路径
mkdir -p "$(dirname "$PLUGIN_DIR")"
rm -rf "$PLUGIN_DIR"
cp -r "$TMP_DIR/weave" "$PLUGIN_DIR"
ok "已安装到 $PLUGIN_DIR"

# ═══════════════════════════════════════════
# [4/6] 配置环境变量
# ═══════════════════════════════════════════
info "[4/6] 配置环境变量..."

# 确保配置文件存在
touch "$ENV_FILE"

# 读取已有值（不覆盖）
get_env() {
    grep "^$1=" "$ENV_FILE" 2>/dev/null | head -1 | cut -d'=' -f2-
}

# 写入环境变量（如果不存在）
set_env_if_absent() {
    local key="$1" val="$2"
    if ! grep -q "^${key}=" "$ENV_FILE" 2>/dev/null; then
        echo "${key}=${val}" >> "$ENV_FILE"
        ok "已写入 ${key}"
    else
        local existing=$(get_env "$key")
        ok "${key} 已存在: ${existing:0:20}...（保留）"
    fi
}

echo ""
echo "  请输入 Weave 连接配置:"
echo ""

# WEAVE_WS_URL
EXISTING_URL=$(get_env "WEAVE_WS_URL")
if [ -n "$EXISTING_URL" ]; then
    ok "WEAVE_WS_URL 已存在: $EXISTING_URL（保留）"
else
    read -p "  Weave WebSocket URL [wss://www.weaveai.chat]: " input_url
    WEAVE_URL="${input_url:-wss://www.weaveai.chat}"
    set_env_if_absent "WEAVE_WS_URL" "$WEAVE_URL"
fi

# WEAVE_WS_ID
EXISTING_ID=$(get_env "WEAVE_WS_ID")
if [ -n "$EXISTING_ID" ]; then
    ok "WEAVE_WS_ID 已存在: ${EXISTING_ID:0:12}...（保留）"
else
    echo ""
    echo "  WS ID 是在 Weave 中创建 AI 联系人时生成的标识符。"
    echo "  获取方式: Weave 网站 → 联系人菜单 → 添加 AI 联系人 → 复制 WS ID"
    echo ""
    while true; do
        read -p "  Weave WS ID: " input_id
        if [ -n "$input_id" ]; then
            set_env_if_absent "WEAVE_WS_ID" "$input_id"
            break
        fi
        warn "WS ID 不能为空"
    done
fi

# WEAVE_API_KEY（可选）
EXISTING_KEY=$(get_env "WEAVE_API_KEY")
if [ -n "$EXISTING_KEY" ]; then
    ok "WEAVE_API_KEY 已存在（保留）"
else
    read -p "  API Key（可选，直接回车跳过）: " input_key
    if [ -n "$input_key" ]; then
        set_env_if_absent "WEAVE_API_KEY" "$input_key"
    else
        info "跳过 API Key（演示模式，无需认证）"
    fi
fi

echo ""

# ═══════════════════════════════════════════
# [5/6] 启用插件
# ═══════════════════════════════════════════
info "[5/6] 启用插件..."

# 方式一：优先用 hermes CLI
if [ -n "$HERMES" ]; then
    # 检查是否已启用
    ENABLED=$("$HERMES" -p "$SELECTED_PROFILE" plugins list 2>/dev/null | grep "weave-platform" | head -1)
    if echo "$ENABLED" | grep -q "enabled"; then
        ok "weave-platform 已启用"
    elif [ -n "$ENABLED" ]; then
        # 已安装但未启用
        "$HERMES" -p "$SELECTED_PROFILE" plugins enable weave-platform 2>/dev/null && ok "已通过 CLI 启用" || {
            warn "CLI 启用失败，尝试手动修改 config.yaml..."
            ENABLE_PLUGIN_MANUALLY=1
        }
    else
        # 未安装到 hermes 的插件系统，手动修改 config.yaml
        ENABLE_PLUGIN_MANUALLY=1
    fi
else
    ENABLE_PLUGIN_MANUALLY=1
fi

# 方式二：手动修改 config.yaml
if [ "${ENABLE_PLUGIN_MANUALLY:-0}" = "1" ]; then
    if [ ! -f "$CONFIG_FILE" ]; then
        # config.yaml 不存在，创建
        cat > "$CONFIG_FILE" << 'YAML'
plugins:
  enabled:
  - weave-platform
  disabled: []
YAML
        ok "已创建 config.yaml 并启用 weave-platform"
    else
        # 检查是否已启用
        if grep -q "weave-platform" "$CONFIG_FILE" 2>/dev/null; then
            ok "config.yaml 中已有 weave-platform"
        else
            # 用 python 安全修改 YAML
            "$PYTHON" - << PYEOF
import yaml, sys

config_path = "$CONFIG_FILE"
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
    # 从 disabled 中移除（如果存在）
    if 'weave-platform' in config['plugins'].get('disabled', []):
        config['plugins']['disabled'].remove('weave-platform')

with open(config_path, 'w') as f:
    yaml.dump(config, f, default_flow_style=False, allow_unicode=True)
print("OK")
PYEOF
            if [ $? -eq 0 ]; then
                ok "已在 config.yaml 中启用 weave-platform"
            else
                warn "自动修改 config.yaml 失败，请手动添加 weave-platform 到 plugins.enabled"
            fi
        fi
    fi
fi

# ═══════════════════════════════════════════
# [6/6] 完成
# ═══════════════════════════════════════════
echo ""
echo -e "${GREEN}${BOLD}╔══════════════════════════════════════════╗${NC}"
echo -e "${GREEN}${BOLD}║          安装完成！                      ║${NC}"
echo -e "${GREEN}${BOLD}╚══════════════════════════════════════════╝${NC}"
echo ""
echo "  配置摘要:"
echo "    Profile:    $SELECTED_PROFILE"
echo "    插件路径:   $PLUGIN_DIR"
echo "    环境变量:   $ENV_FILE"
echo "    WS URL:     $(get_env WEAVE_WS_URL)"
echo "    WS ID:      $(get_env WEAVE_WS_ID | cut -c1-12)..."
echo ""
echo -e "  ${BOLD}下一步${NC}: 重启网关使插件生效"
echo ""
echo "    hermes -p $SELECTED_PROFILE gateway run --replace"
echo ""
echo "  详细文档: https://www.weaveai.chat/docs/hermes"
echo ""
