#!/usr/bin/env bash
# racewong 客户端本地编译脚本（WSL / Linux / macOS / Git Bash 均可）
#
# 干什么：
#   WSL 里默认走 cargo-xwin 交叉编译到 x86_64-pc-windows-msvc，
#   产物是能直接在 Windows 上双击运行的 racewong-app.exe（和 app/README.md 同一条链路）；
#   非 WSL 环境默认构建本机原生二进制。
#
# 用法：
#   ./script/build.sh                  # release 构建（WSL 下自动交叉编译到 Windows）
#   ./script/build.sh --debug          # debug 构建（带控制台，方便看日志/打印）
#   ./script/build.sh --native         # 只构建当前系统原生版本（WSL 下就是 Linux 版）
#   ./script/build.sh --run            # 构建完直接启动客户端
#   ./script/build.sh --check          # 构建完跑 --check 自检（PG 试写回滚 + MQTT CONNACK）
#   ./script/build.sh --test           # 先跑协议单测（cargo test --bins）
#   ./script/build.sh --clean          # 先 cargo clean 再编译
#   ./script/build.sh --no-kill        # 不要自动结束还在跑的客户端（默认会自动结束）
#   ./script/build.sh --target <triple>  # 指定目标三元组
#
# 关于 os error 5：
#   Windows 上正在运行的 exe 无法被删除/覆盖，cargo 写产物（或 cargo clean）时会报
#   "failed to remove file ... Input/output error (os error 5)"。
#   交叉编译 Windows 目标时，本脚本会在编译前通过 tasklist/taskkill（WSL 互操作）
#   检查并结束还在跑的 racewong-app.exe；互操作不可用时会给出提示。
#
# 依赖：
#   cargo；在 Linux/WSL 上编 Windows 目标还需要
#     rustup target add x86_64-pc-windows-msvc
#     cargo install cargo-xwin
#   MSVC CRT/SDK 缓存目录默认 /mnt/d/xwin-cache（可用 XWIN_CACHE_DIR 覆盖），不占 C 盘。

set -euo pipefail

cd "$(dirname "$0")/.."
REPO_ROOT="$(pwd)"
APP_DIR="app"
BIN_NAME="racewong-app"

TARGET=""
NATIVE=0
PROFILE="release"
DO_RUN=0
DO_CHECK=0
DO_TEST=0
DO_CLEAN=0
DO_KILL=1

die() { echo "错误: $*" >&2; exit 1; }
step() { echo; echo "==> $*"; }
ok() { echo "   $*"; }
warn() { echo "!  $*"; }
have() { command -v "$1" >/dev/null 2>&1; }

usage() {
    cat <<'EOF'
racewong 客户端本地编译脚本

用法:
  ./script/build.sh [选项]

选项:
  --debug              debug 构建（带控制台，方便看日志）
  --release            release 构建（默认）
  --native             只构建当前系统原生版本（WSL 下是 Linux 版，不是 exe）
  --run                构建完成后启动客户端
  --check              构建完成后跑 --check 自检（PostgreSQL 试写回滚 + MQTT CONNACK）
  --test               编译前先跑协议单测
  --clean              编译前 cargo clean
  --no-kill            不要自动结束还在运行的客户端（默认会自动结束，避免 os error 5）
  --target <triple>    指定目标三元组，如 x86_64-pc-windows-msvc
  -h, --help           显示本帮助
EOF
}

while [ $# -gt 0 ]; do
    case "$1" in
        --debug) PROFILE="debug"; shift ;;
        --release) PROFILE="release"; shift ;;
        --native) NATIVE=1; shift ;;
        --run) DO_RUN=1; shift ;;
        --check) DO_CHECK=1; shift ;;
        --test) DO_TEST=1; shift ;;
        --clean) DO_CLEAN=1; shift ;;
        --no-kill) DO_KILL=0; shift ;;
        --target)
            if [ $# -lt 2 ]; then die "--target 后面要跟目标三元组"; fi
            TARGET="$2"
            shift 2
            ;;
        --target=*) TARGET="${1#*=}"; shift ;;
        -h|--help) usage; exit 0 ;;
        *) die "未知参数: $1（用 --help 看用法）" ;;
    esac
done

is_wsl() {
    if [ -n "${WSL_DISTRO_NAME:-}" ]; then return 0; fi
    if [ -r /proc/version ] && grep -qi microsoft /proc/version 2>/dev/null; then return 0; fi
    return 1
}

# ---------------------------------------------------------------- 目标平台
if [ -z "$TARGET" ] && [ "$NATIVE" = "0" ] && is_wsl; then
    TARGET="x86_64-pc-windows-msvc"
fi

IS_WINDOWS_TARGET=0
case "$TARGET" in
    *windows*) IS_WINDOWS_TARGET=1 ;;
esac

USE_XWIN=0
if [ "$IS_WINDOWS_TARGET" = "1" ] && [ "$(uname -s)" = "Linux" ]; then
    USE_XWIN=1
fi

have cargo || die "找不到 cargo，请先装 Rust 工具链（https://rustup.rs）"

if [ "$USE_XWIN" = "1" ]; then
    if ! cargo xwin --version >/dev/null 2>&1; then
        die "交叉编译 Windows 目标需要 cargo-xwin：cargo install cargo-xwin（再确认已 rustup target add $TARGET）"
    fi
    if [ -z "${XWIN_CACHE_DIR:-}" ]; then
        if [ -d /mnt/d ]; then
            XWIN_CACHE_DIR="/mnt/d/xwin-cache"
        else
            XWIN_CACHE_DIR="$HOME/.cache/xwin"
        fi
    fi
    export XWIN_CACHE_DIR
    mkdir -p "$XWIN_CACHE_DIR"
fi

# ---------------------------------------------------------------- 结束占用进程
# Windows 上正在运行的 exe 无法被删除/覆盖，cargo 写产物（甚至 cargo clean）时
# 会报 "failed to remove file ... Input/output error (os error 5)"。
# WSL 里借 Windows 侧的 tasklist / taskkill（互操作）处理。
WIN_TASKLIST=""
WIN_TASKKILL=""

win_tool() {
    if [ -x "/mnt/c/Windows/System32/$1" ]; then
        echo "/mnt/c/Windows/System32/$1"
        return 0
    fi
    if have "$1"; then
        command -v "$1"
        return 0
    fi
    return 1
}

app_running() {
    [ -n "$WIN_TASKLIST" ] || return 1
    # 用 - 风格参数（Git Bash 会把 /FI 当路径转换掉）
    # -FO CSV 命中时形如 "racewong-app.exe","1234","Console","1","12,345 K"；
    # 没命中时是一行本地化提示文字，不含带引号的进程名，所以按带引号匹配最稳。
    "$WIN_TASKLIST" -FO CSV -NH -FI "IMAGENAME eq ${BIN_NAME}.exe" 2>/dev/null \
        | grep -qi "\"${BIN_NAME}\.exe\""
}

stop_running_app() {
    app_running || return 0
    if [ "$DO_KILL" != "1" ] || [ -z "$WIN_TASKKILL" ]; then
        warn "检测到 ${BIN_NAME}.exe 正在运行，编译时可能报 os error 5（产物被占用）"
        warn "先在 Windows 托盘菜单里点「退出」再重试，或去掉 --no-kill 让本脚本自动结束它"
        return 0
    fi
    warn "检测到 ${BIN_NAME}.exe 正在运行，先结束它再编译"
    "$WIN_TASKKILL" -F -IM "${BIN_NAME}.exe" >/dev/null 2>&1 || true
    n=0
    while [ "$n" -lt 20 ]; do
        app_running || break
        sleep 0.2
        n=$((n + 1))
    done
    if app_running; then
        warn "结束进程失败（可能权限不足）；先在托盘里退出客户端，或用任务管理器结束 ${BIN_NAME}.exe"
    else
        ok "已结束旧进程"
    fi
}

if [ "$IS_WINDOWS_TARGET" = "1" ]; then
    WIN_TASKLIST="$(win_tool tasklist.exe || true)"
    WIN_TASKKILL="$(win_tool taskkill.exe || true)"
    if [ -z "$WIN_TASKLIST" ]; then
        warn "拿不到 Windows 侧的 tasklist（WSL 互操作可能被关掉），无法自动结束旧进程"
        warn "若编译报 os error 5，先在 Windows 里退出客户端再重试"
    else
        stop_running_app
    fi
fi

cd "$APP_DIR"

if [ "$DO_CLEAN" = "1" ]; then
    step "清理旧产物"
    cargo clean
fi

if [ "$DO_TEST" = "1" ]; then
    step "协议单测"
    cargo test --bins
fi

CMD=(cargo)
if [ "$USE_XWIN" = "1" ]; then
    CMD+=(xwin)
fi
CMD+=(build)
if [ "$PROFILE" = "release" ]; then
    CMD+=(--release)
fi
if [ -n "$TARGET" ]; then
    CMD+=(--target "$TARGET")
fi

step "编译（$PROFILE，目标: ${TARGET:-本机}）"
ok "执行: ${CMD[*]}"
if [ "$USE_XWIN" = "1" ]; then
    ok "MSVC 缓存目录: $XWIN_CACHE_DIR"
fi
if ! "${CMD[@]}"; then
    if [ "$IS_WINDOWS_TARGET" = "1" ] && app_running; then
        warn "${BIN_NAME}.exe 还在运行，产物无法覆盖（os error 5）"
        warn "先在托盘菜单里点「退出」再重试，或去掉 --no-kill 让本脚本自动结束它"
    fi
    die "编译失败"
fi

# ---------------------------------------------------------------- 产物
BIN="$REPO_ROOT/$APP_DIR/target"
if [ -n "$TARGET" ]; then
    BIN="$BIN/$TARGET"
fi
BIN="$BIN/$PROFILE/$BIN_NAME"
if [ "$IS_WINDOWS_TARGET" = "1" ]; then
    BIN="$BIN.exe"
fi

if [ ! -f "$BIN" ]; then
    die "编译完成但没找到产物: $BIN"
fi

step "产物"
SIZE="$(du -h "$BIN" | cut -f1 | tr -d ' ')"
ok "$BIN（$SIZE）"
if [ "$IS_WINDOWS_TARGET" = "1" ] && have wslpath; then
    ok "Windows 里看: $(wslpath -w "$BIN")"
fi

# Windows 目标只能从 /mnt/* 下启动（WSL 互操作），ext4 里的 exe 跑不起来
RUNNABLE=1
if [ "$IS_WINDOWS_TARGET" = "1" ]; then
    case "$BIN" in
        /mnt/*) ;;
        *)
            RUNNABLE=0
            warn "产物在 Linux 文件系统里，Windows 侧无法直接启动；把仓库放到 /mnt/d 下再编译，或手动拷到 Windows 运行"
            ;;
    esac
fi

if [ "$DO_CHECK" = "1" ] && [ "$RUNNABLE" = "1" ]; then
    step "连通性自检（--check）"
    if ! "$BIN" --check; then
        warn "自检没通过，按上面的输出排查（连接串、密码、表结构）"
    fi
fi

if [ "$DO_RUN" = "1" ] && [ "$RUNNABLE" = "1" ]; then
    step "启动客户端"
    # Windows 目标用 cmd start 脱离当前终端：托盘常驻时关掉终端不会把客户端一起带走，
    # 终端也能立刻空出来跑下一次编译。
    if [ "$IS_WINDOWS_TARGET" = "1" ] && have wslpath; then
        WIN_BIN="$(wslpath -w "$BIN")"
        if [ -x /mnt/c/Windows/System32/cmd.exe ]; then
            if /mnt/c/Windows/System32/cmd.exe /c start "" "$WIN_BIN" >/dev/null 2>&1; then
                ok "已在后台启动: $WIN_BIN"
            else
                warn "启动失败，手动双击运行: $WIN_BIN"
            fi
            exit 0
        fi
    fi
    exec "$BIN"
fi
