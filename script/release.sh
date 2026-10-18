#!/usr/bin/env bash
# racewong 发布脚本（bash 版，Linux / macOS / WSL / Git Bash 均可跑）
#
# 完整发布流程：
#   读取/递增版本 -> 同步 app/Cargo.toml -> 提交 -> 打标签 -> 推送
#   -> 用 git 提交记录生成 Release 说明 -> 创建/更新 GitHub Release -> 上传安装包
#
# 三个平台的安装包（本脚本只构建"当前这台机器"能构建的那一个，其余交给 GitHub Actions）：
#   Windows x86-64 : racewong-<ver>-windows-x86_64.zip
#   macOS   arm64  : racewong-<ver>-macos-aarch64.tar.gz（在 macOS 上还会出 .dmg）
#   Linux   x86-64 : racewong-<ver>-linux-x86_64.tar.gz（有 dpkg-deb 时还会出 .deb）
#
# 用法：
#   ./script/release.sh                       # 自动 patch +1，走完整流程
#   ./script/release.sh --version v1.2.0      # 指定版本（也可以不带 v）
#   ./script/release.sh --no-build            # 只打标签 + 建 Release，包全交给 CI
#   ./script/release.sh --build-only          # 只构建当前平台安装包（CI 用）
#   ./script/release.sh --notes-only          # 只输出 git 提交记录（当 Release 正文）
#   ./script/release.sh --skip-tests --dry-run
#
# 需要：git、cargo（构建时）、gh（建 Release 时；没装也可以用 GITHUB_TOKEN 走 REST）

set -euo pipefail

cd "$(dirname "$0")/.."
REPO_ROOT="$(pwd)"
APP_DIR="app"
BIN_NAME="racewong-app"

VERSION=""
BUILD_ONLY=0
NO_BUILD=0
NOTES_ONLY=0
SKIP_TESTS=0
FORCE=0
DRY_RUN=0
OUT_DIR="dist"

while [ $# -gt 0 ]; do
  case "$1" in
    --version|-v)      VERSION="${2:-}"; shift 2 ;;
    --build-only)      BUILD_ONLY=1; shift ;;
    --no-build)        NO_BUILD=1; shift ;;
    --notes-only)      NOTES_ONLY=1; shift ;;
    --skip-tests)      SKIP_TESTS=1; shift ;;
    --force)           FORCE=1; shift ;;
    --dry-run)         DRY_RUN=1; shift ;;
    --out-dir)         OUT_DIR="${2:-}"; shift 2 ;;
    -h|--help)         sed -n '2,30p' "$0"; exit 0 ;;
    *)                 echo "未知参数: $1（用 --help 看用法）" >&2; exit 1 ;;
  esac
done

say()  { printf '%s\n' "$*"; }
step() { printf '\033[36m==> %s\033[0m\n' "$*"; }
ok()   { printf '\033[32m   %s\033[0m\n' "$*"; }
warn() { printf '\033[33m!  %s\033[0m\n' "$*"; }
die()  { printf '\033[31m错误: %s\033[0m\n' "$*" >&2; exit 1; }

have() { command -v "$1" >/dev/null 2>&1; }

run() { # run <cmd> [args...]  —— dry-run 时只打印
  if [ "$DRY_RUN" = "1" ]; then
    printf '   [dry-run] %s\n' "$*"
    return 0
  fi
  "$@"
}

host_os() {
  case "$(uname -s)" in
    Darwin)  echo macos ;;
    Linux)   echo linux ;;
    MINGW*|MSYS*|CYGWIN*) echo windows ;;
    *)       echo linux ;;
  esac
}

host_arch() {
  local m; m="$(uname -m)"
  case "$m" in
    aarch64|arm64) echo aarch64 ;;
    x86_64|amd64)  echo x86_64 ;;
    *)             echo "$m" ;;
  esac
}

host_target() {
  local os arch; os="$(host_os)"; arch="$(host_arch)"
  case "$os" in
    windows) echo x86_64-pc-windows-msvc ;;
    macos)   echo aarch64-apple-darwin ;;
    *)       if [ "$arch" = "aarch64" ]; then echo aarch64-unknown-linux-gnu; else echo x86_64-unknown-linux-gnu; fi ;;
  esac
}

os_label()   { case "$1" in *windows*) echo windows ;; *darwin*) echo macos ;; *) echo linux ;; esac; }
arch_label() { case "$1" in aarch64*) echo aarch64 ;; *) echo x86_64 ;; esac; }

cargo_version() {
  awk '/^version = "/ { gsub(/"/, "", $3); print $3; exit }' "$APP_DIR/Cargo.toml"
}

set_cargo_version() { # set_cargo_version <ver>
  local ver="$1" tmp; tmp="$(mktemp)"
  awk -v v="$ver" '!d && /^version = "/ { print "version = \"" v "\""; d=1; next } { print }' \
    "$APP_DIR/Cargo.toml" > "$tmp"
  if cmp -s "$tmp" "$APP_DIR/Cargo.toml"; then rm -f "$tmp"; return 1; fi
  mv "$tmp" "$APP_DIR/Cargo.toml"
  return 0
}

latest_tag() { git tag --list 'v*.*.*' --sort=-version:refname | head -n1; }

next_version() { # next_version <latest-tag>
  local latest="$1"
  if [ -z "$latest" ]; then cargo_version; return; fi
  if [[ "$latest" =~ ^v([0-9]+)\.([0-9]+)\.([0-9]+)$ ]]; then
    echo "${BASH_REMATCH[1]}.${BASH_REMATCH[2]}.$(( ${BASH_REMATCH[3]} + 1 ))"
  else
    cargo_version
  fi
}

release_notes() { # release_notes <from> <to>
  local from="$1" to="$2" range
  if [ -n "$from" ]; then range="$from..$to"; else range="$to"; fi
  if [ -n "$from" ]; then printf '## 变更记录（%s .. %s）\n' "$from" "$to"; else printf '## 变更记录\n'; fi
  local lines
  lines="$(git log --no-merges --pretty='format:* %s (%h)' "$range")"
  if [ -z "$lines" ]; then printf '本次发布没有新的提交记录。\n'; else printf '%s\n' "$lines"; fi
}

build_app() { # build_app <target>
  local target="$1"
  step "构建 $target"
  have cargo || die '找不到 cargo，请先安装 Rust 工具链'
  ( cd "$APP_DIR"
    if [ "$SKIP_TESTS" = "0" ]; then run cargo test --bins --release || exit 1; fi
    run cargo build --release --locked --target "$target" || exit 1
  ) || die "构建失败（target=$target）"
  ok '构建完成'
}

make_app_bundle() { # make_app_bundle <stage> <bin> <ver>
  local stage="$1" bin="$2" ver="$3"
  local contents="$stage/RaceBox 同步.app/Contents"
  mkdir -p "$contents/MacOS" "$contents/Resources"
  cp "$bin" "$contents/MacOS/$BIN_NAME"
  [ -f "$APP_DIR/assets/icon256.png" ] && cp "$APP_DIR/assets/icon256.png" "$contents/Resources/icon256.png"
  cat > "$contents/Info.plist" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>CFBundleName</key><string>RaceBox 同步</string>
  <key>CFBundleDisplayName</key><string>RaceBox 同步</string>
  <key>CFBundleIdentifier</key><string>com.wangcw.racewong</string>
  <key>CFBundleExecutable</key><string>$BIN_NAME</string>
  <key>CFBundleVersion</key><string>$ver</string>
  <key>CFBundleShortVersionString</key><string>$ver</string>
  <key>CFBundlePackageType</key><string>APPL</string>
  <key>LSMinimumSystemVersion</key><string>11.0</string>
  <key>NSHighResolutionCapable</key><true/>
  <key>NSBluetoothAlwaysUsageDescription</key><string>需要蓝牙权限以连接 RaceBox 设备</string>
</dict>
</plist>
PLIST
}

make_deb() { # make_deb <stage> <bin> <ver> <out.deb>
  local stage="$1" bin="$2" ver="$3" out="$4"
  local root="${stage}-deb"
  rm -rf "$root"
  mkdir -p "$root/DEBIAN" "$root/usr/bin" "$root/usr/share/doc/$BIN_NAME"
  cp "$bin" "$root/usr/bin/$BIN_NAME"
  [ -f "$APP_DIR/README.md" ] && cp "$APP_DIR/README.md" "$root/usr/share/doc/$BIN_NAME/README.md"
  cat > "$root/DEBIAN/control" <<CTRL
Package: $BIN_NAME
Version: $ver
Section: utils
Priority: optional
Architecture: amd64
Maintainer: wangcw <rubygreat@msn.com>
Homepage: https://github.com/redgreat/racewong
Description: RaceBox 同步桌面应用
 通过蓝牙连接 RaceBox Mini S，下载轨迹数据并同步到 PostgreSQL / MQTT，
 上传成功后自动清理设备内存。
CTRL
  if dpkg-deb --build --root-owner-group "$root" "$out" >/dev/null 2>&1 \
     || dpkg-deb --build "$root" "$out" >/dev/null 2>&1; then
    rm -rf "$root"; return 0
  fi
  rm -rf "$root"; return 1
}

make_zip() { # make_zip <stage> <out.zip>
  local stage="$1" out="$2" abs_out="$2"
  case "$abs_out" in /*) ;; *) abs_out="$REPO_ROOT/$abs_out" ;; esac
  if have zip; then
    ( cd "$stage" && zip -qr "$abs_out" . )
    return $?
  fi
  if have powershell || have pwsh; then
    local ps; ps="$(command -v pwsh || command -v powershell)"
    "$ps" -NoProfile -Command "Compress-Archive -Path '$stage/*' -DestinationPath '$out' -Force"
    return $?
  fi
  return 1
}

make_package() { # make_package <target> <ver>  —— 输出产出的文件（每行一个）
  local target="$1" ver="$2"
  local os arch name stage bin
  os="$(os_label "$target")"; arch="$(arch_label "$target")"
  name="racewong-$ver-$os-$arch"
  stage="$OUT_DIR/$name"
  rm -rf "$stage"; mkdir -p "$stage"

  bin="$APP_DIR/target/$target/release/$BIN_NAME"
  [ "$os" = "windows" ] && bin="$bin.exe"
  [ -f "$bin" ] || die "找不到构建产物: $bin"

  if [ "$os" = "macos" ]; then
    make_app_bundle "$stage" "$bin" "$ver"
  else
    cp "$bin" "$stage/$BIN_NAME"
    if [ "$os" = "windows" ] && [ -f "$APP_DIR/assets/icon.ico" ]; then
      cp "$APP_DIR/assets/icon.ico" "$stage/"
    fi
  fi
  [ -f "$APP_DIR/README.md" ] && cp "$APP_DIR/README.md" "$stage/README.txt"
  [ -f LICENSE ] && cp LICENSE "$stage/"

  if [ "$os" = "windows" ]; then
    local zip="$OUT_DIR/$name.zip"
    rm -f "$zip"
    make_zip "$stage" "$zip" || die '打包 zip 失败'
    ok "产出 $name.zip"
    echo "$zip"
  else
    local tgz="$OUT_DIR/$name.tar.gz"
    rm -f "$tgz"
    tar -czf "$tgz" -C "$OUT_DIR" "$name" || die '打包 tar.gz 失败'
    ok "产出 $name.tar.gz"
    echo "$tgz"

    if [ "$os" = "macos" ] && [ "$(host_os)" = "macos" ] && have hdiutil; then
      local dmg="$OUT_DIR/$name.dmg"
      rm -f "$dmg"
      if hdiutil create -volname "RaceBox 同步 $ver" -srcfolder "$stage" -ov -format UDZO "$dmg" >/dev/null; then
        ok "产出 $name.dmg"
        echo "$dmg"
      else
        warn '生成 dmg 失败，仅保留 tar.gz'
      fi
    fi
    if [ "$os" = "linux" ] && have dpkg-deb; then
      local deb="$OUT_DIR/$name.deb"
      rm -f "$deb"
      if make_deb "$stage" "$bin" "$ver" "$deb"; then
        ok "产出 $name.deb"
        echo "$deb"
      else
        warn '生成 deb 失败，仅保留 tar.gz'
      fi
    fi
  fi
}

publish_release() { # publish_release <tag> <notes> <assets...>
  local tag="$1"; shift
  local notes="$1"; shift
  local assets=("$@")
  mkdir -p "$OUT_DIR"
  printf '%s\n' "$notes" > "$OUT_DIR/release-notes.md"

  if have gh; then
    if ! gh release view "$tag" >/dev/null 2>&1; then
      step "创建 GitHub Release $tag"
      local args=(release create "$tag" --title "RaceBox 同步 $tag" --notes-file "$OUT_DIR/release-notes.md")
      [[ "$tag" == *-* ]] && args+=(--prerelease)
      run gh "${args[@]}" || die '创建 Release 失败'
      [ "$DRY_RUN" = "0" ] && ok 'Release 已创建'
    else
      ok "Release $tag 已存在，直接上传附件"
    fi
    if [ ${#assets[@]} -gt 0 ]; then
      step "上传 ${#assets[@]} 个安装包"
      run gh release upload "$tag" "${assets[@]}" --clobber || die '上传附件失败'
      [ "$DRY_RUN" = "0" ] && ok '附件已上传'
    fi
    return 0
  fi

  local token="${GITHUB_TOKEN:-${GH_TOKEN:-}}"
  if [ -z "$token" ]; then
    warn '未安装 gh 且没有 GITHUB_TOKEN，跳过 Release；说明已写入 dist/release-notes.md'
    return 0
  fi
  local remote repo
  remote="$(git remote get-url origin 2>/dev/null || true)"
  [ -n "$remote" ] || { warn '没有 origin 远端，跳过 Release'; return 0; }
  repo="$(printf '%s' "$remote" | sed -E 's#^.*github\.com[:/](.+)$#\1#; s#\.git$##')"

  step "通过 REST 创建/更新 GitHub Release $tag"
  local release_id
  release_id="$(curl -fsS -H "Authorization: Bearer $token" -H 'Accept: application/vnd.github+json' \
    "https://api.github.com/repos/$repo/releases/tags/$tag" | sed -n 's/^  "id": \([0-9]*\),$/\1/p' | head -n1)"
  if [ -z "$release_id" ]; then
    if [ "$DRY_RUN" = "1" ]; then
      printf '   [dry-run] POST /repos/%s/releases\n' "$repo"
      return 0
    fi
    local prerelease=false
    [[ "$tag" == *-* ]] && prerelease=true
    release_id="$(curl -fsS -X POST -H "Authorization: Bearer $token" -H 'Accept: application/vnd.github+json' \
      -H 'Content-Type: application/json' \
      -d "$(printf '{"tag_name":"%s","name":"RaceBox 同步 %s","body":%s,"draft":false,"prerelease":%s}' \
             "$tag" "$tag" "$(printf '%s' "$notes" | python3 -c 'import json,sys; print(json.dumps(sys.stdin.read()))')" "$prerelease")" \
      "https://api.github.com/repos/$repo/releases" | sed -n 's/^  "id": \([0-9]*\),$/\1/p' | head -n1)"
    ok 'Release 已创建'
  fi
  [ -n "$release_id" ] || { warn '创建 Release 失败'; return 0; }
  local a
  for a in "${assets[@]}"; do
    run curl -fsS -X POST -H "Authorization: Bearer $token" -H 'Content-Type: application/octet-stream' \
      --data-binary "@$a" \
      "https://uploads.github.com/repos/$repo/releases/$release_id/assets?name=$(basename "$a")" >/dev/null
    [ "$DRY_RUN" = "0" ] && ok "已上传 $(basename "$a")"
  done
}

# ---------------------------------------------------------------- 主流程
have git || die '找不到 git'

if [ "$NOTES_ONLY" = "1" ]; then
  to_ref="HEAD"
  if [ -n "$VERSION" ]; then
    v="$VERSION"; [[ "$v" == v* ]] || v="v$v"
    to_ref="$v"
  fi
  tags=()
  while IFS= read -r line; do [ -n "$line" ] && tags+=("$line"); done < <(git tag --list 'v*.*.*' --sort=-version:refname)
  cur="$(git describe --tags --exact-match HEAD 2>/dev/null || true)"
  from=""
  if [ -n "$cur" ]; then
    [ ${#tags[@]} -gt 1 ] && from="${tags[1]}"
  elif [ ${#tags[@]} -gt 0 ]; then
    from="${tags[0]}"
  fi
  release_notes "$from" "$to_ref"
  exit 0
fi

if [ "$BUILD_ONLY" = "1" ]; then
  ver="$(cargo_version)"
  target="$(host_target)"
  build_app "$target"
  step '完成'
  make_package "$target" "$ver" | while read -r f; do [ -n "$f" ] && echo "$(cd "$(dirname "$f")" && pwd)/$(basename "$f")"; done
  exit 0
fi

step '检查工作区'
branch="$(git rev-parse --abbrev-ref HEAD)"
if [ -n "$(git status --porcelain)" ] && [ "$FORCE" = "0" ]; then
  die "工作区不干净，请先提交或暂存改动（或用 --force 忽略）"
fi
ok "分支 $branch"
git fetch --tags origin >/dev/null 2>&1 || true

if [ -z "$VERSION" ]; then
  latest="$(latest_tag)"
  ver="$(next_version "$latest")"
  if [ -n "$latest" ]; then ok "自动版本：$latest -> v$ver"; else ok "自动版本：v$ver（无历史标签）"; fi
else
  ver="$VERSION"
  ver="${ver#v}"
  [[ "$ver" =~ ^[0-9]+\.[0-9]+\.[0-9]+ ]] || die "版本格式错误: $VERSION（需要 v主版本.次版本.修订号）"
fi
tag="v$ver"
git tag --list "$tag" | grep -q . && die "标签 $tag 已存在"
ok "目标版本 $tag"

prev_tag="$(latest_tag)"
notes="$(release_notes "$prev_tag" HEAD)"

step '同步 app/Cargo.toml 版本'
old_ver="$(cargo_version)"
if [ "$old_ver" != "$ver" ]; then
  if [ "$DRY_RUN" = "1" ]; then
    printf '   [dry-run] app/Cargo.toml: %s -> %s\n' "$old_ver" "$ver"
  else
    set_cargo_version "$ver"
    run git add "$APP_DIR/Cargo.toml" || die 'git add 失败'
    run git commit -m "chore(release): bump version to $ver" || die '提交失败'
    ok "版本号 $old_ver -> $ver"
  fi
else
  ok "版本号已是 $ver"
fi

step "打标签 $tag"
if [ "$DRY_RUN" = "1" ]; then
  printf "   [dry-run] git tag -a %s -m 'Release %s'\n" "$tag" "$tag"
else
  run git tag -a "$tag" -m "Release $tag" || die '创建标签失败'
  ok '标签已创建'
fi

step '推送提交与标签'
if [ "$DRY_RUN" = "1" ]; then
  printf "   [dry-run] git push origin %s && git push origin %s\n" "$branch" "$tag"
else
  git push origin "$branch" || warn "推送分支 $branch 失败（可能没有上游），继续推送标签"
  run git push origin "$tag" || die '推送标签失败'
  ok "已推送 $tag"
fi

assets=()
if [ "$NO_BUILD" = "0" ]; then
  target="$(host_target)"
  build_app "$target"
  while read -r f; do [ -n "$f" ] && assets+=("$f"); done < <(make_package "$target" "$ver")
else
  ok '--no-build：本机不构建，三个平台的包由 GitHub Actions 产出'
fi

if [ ${#assets[@]} -gt 0 ]; then
  publish_release "$tag" "$notes" "${assets[@]}"
else
  publish_release "$tag" "$notes"
fi

step '发布完成'
say "  Release : https://github.com/redgreat/racewong/releases/tag/$tag"
if [ -n "$prev_tag" ]; then say "  说明来源: git 提交记录（$prev_tag..HEAD）"; else say '  说明来源: git 提交记录（全部历史）'; fi
if [ "$NO_BUILD" = "1" ] || [ ${#assets[@]} -eq 0 ]; then
  say '  GitHub Actions 会构建 win-x86_64 / macos-aarch64 / linux-x86_64 三个包并上传到该 Release'
fi
