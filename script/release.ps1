#!/usr/bin/env pwsh
#Requires -Version 5.1
# racewong 发布脚本（兼容 Windows PowerShell 5.1 与 PowerShell 7+）
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
#   .\script\release.ps1                      # 自动 patch +1，走完整流程
#   .\script\release.ps1 -Version v1.2.0      # 指定版本
#   .\script\release.ps1 -Version 1.2.0       # 也可以不带 v
#   .\script\release.ps1 -NoBuild             # 只打标签 + 建 Release，包全交给 CI
#   .\script\release.ps1 -BuildOnly           # 只构建当前平台安装包（CI 用），不做 git/gh 操作
#   .\script\release.ps1 -NotesOnly           # 只输出 git 提交记录（当 Release 正文）
#   .\script\release.ps1 -SkipTests -DryRun   # 演示：跳过测试、只打印不执行
#
# 需要：git、cargo（构建时）、gh（建 Release 时；没装也可以用 GITHUB_TOKEN 走 REST）

param(
    [Parameter(Position = 0)][string]$Version = '',
    [switch]$BuildOnly,
    [switch]$NoBuild,
    [switch]$NotesOnly,
    [switch]$SkipTests,
    [switch]$Force,
    [switch]$DryRun,
    [string]$OutDir = 'dist'
)

$ErrorActionPreference = 'Stop'
# PowerShell 7.4+ 默认把外部命令的非零退出码当终止错误，这里关掉，统一用 $LASTEXITCODE 判断
if (Get-Variable -Name PSNativeCommandUseErrorActionPreference -ErrorAction SilentlyContinue) {
    $PSNativeCommandUseErrorActionPreference = $false
}
Set-Location (Split-Path -Parent $PSScriptRoot)

$AppDir  = 'app'
$BinName = 'racewong-app'
$RepoRoot = (Get-Location).Path

# ---------------------------------------------------------------- 输出辅助
function Say($m)  { Write-Host $m }
function Step($m) { Write-Host "==> $m" -ForegroundColor Cyan }
function Ok($m)   { Write-Host "   $m" -ForegroundColor Green }
function Warn($m) { Write-Host "!  $m" -ForegroundColor Yellow }
function Die($m)  { Write-Host "错误: $m" -ForegroundColor Red; exit 1 }

function Write-Utf8File([string]$Path, [string]$Text) {
    $dir = Split-Path -Parent $Path
    if ($dir -and -not (Test-Path $dir)) { New-Item -ItemType Directory -Path $dir -Force | Out-Null }
    $enc = New-Object System.Text.UTF8Encoding($false)
    [System.IO.File]::WriteAllText($Path, $Text, $enc)
}

function Get-Tool([string]$Name) {
    return (Get-Command $Name -ErrorAction SilentlyContinue)
}

function Invoke-Checked([string]$Exe, [string[]]$ExeArgs) {
    & $Exe @ExeArgs
    return $LASTEXITCODE
}

# ---------------------------------------------------------------- 平台识别
function Get-HostOS {
    # PowerShell 5.1 只跑在 Windows 上，6.0+ 才有 $IsWindows/$IsMacOS/$IsLinux
    if ($PSVersionTable.PSVersion.Major -ge 6) {
        if ($IsWindows) { return 'windows' }
        if ($IsMacOS)   { return 'macos' }
        return 'linux'
    }
    return 'windows'
}

function Get-HostArch {
    if (Get-Tool 'uname') {
        $m = (& uname -m 2>$null | Select-Object -First 1)
        if ($m) {
            $m = $m.Trim().ToLower()
            if ($m -match '^(aarch64|arm64)$') { return 'aarch64' }
            if ($m -match '^(x86_64|amd64)$')  { return 'x86_64' }
            return $m
        }
    }
    if ($env:PROCESSOR_ARCHITECTURE -match 'ARM64') { return 'aarch64' }
    if ($env:PROCESSOR_ARCHITECTURE -match '64')    { return 'x86_64' }
    return 'x86'
}

function Get-TargetForHost {
    $arch = Get-HostArch
    switch (Get-HostOS) {
        'windows' { return 'x86_64-pc-windows-msvc' }
        'macos'   { return 'aarch64-apple-darwin' }
        default   { if ($arch -eq 'aarch64') { return 'aarch64-unknown-linux-gnu' } else { return 'x86_64-unknown-linux-gnu' } }
    }
}

function Get-OsLabel([string]$Target) {
    if ($Target -like '*windows*') { return 'windows' }
    if ($Target -like '*darwin*')  { return 'macos' }
    return 'linux'
}

function Get-ArchLabel([string]$Target) {
    if ($Target -like 'aarch64*') { return 'aarch64' }
    return 'x86_64'
}

# ---------------------------------------------------------------- 版本
function Get-CargoVersion {
    $cargo = Get-Content (Join-Path $AppDir 'Cargo.toml') -Raw
    if ($cargo -match '(?m)^version\s*=\s*"([^"]+)"') { return $Matches[1] }
    Die '无法从 app/Cargo.toml 读取版本号'
}

function Get-LatestTag {
    $tags = @(& git tag --list 'v*.*.*' --sort=-version:refname)
    if ($tags.Count -gt 0) { return $tags[0] }
    return ''
}

function Get-NextVersion([string]$Latest) {
    if (-not $Latest) { return Get-CargoVersion }
    if ($Latest -match '^v(\d+)\.(\d+)\.(\d+)$') {
        return "$($Matches[1]).$($Matches[2]).$([int]$Matches[3] + 1)"
    }
    return Get-CargoVersion
}

function Set-CargoVersion([string]$Ver) {
    $path = Join-Path $AppDir 'Cargo.toml'
    $text = Get-Content $path -Raw
    $new  = [regex]::Replace($text, '(?m)^version\s*=\s*"[^"]+"', "version = `"$Ver`"", 1)
    if ($new -eq $text) { return $false }
    Write-Utf8File $path $new
    return $true
}

# ---------------------------------------------------------------- Release 正文（git 备注）
function Get-ReleaseNotes([string]$From, [string]$To) {
    $range = if ($From) { "$From..$To" } else { $To }
    $lines = @(& git log --no-merges "--pretty=format:* %s (%h)" $range)
    if ($lines.Count -eq 0) { return "本次发布没有新的提交记录。" }
    $head = if ($From) { "## 变更记录（$From .. $To）" } else { "## 变更记录" }
    return ($head + "`n" + ($lines -join "`n"))
}

# ---------------------------------------------------------------- 构建与打包
function Invoke-Build([string]$Target) {
    Step "构建 $Target"
    if (-not (Get-Tool 'cargo')) { Die '找不到 cargo，请先安装 Rust 工具链' }
    Push-Location $AppDir
    $code = 0
    if (-not $SkipTests) {
        $code = Invoke-Checked 'cargo' @('test', '--bins', '--release')
    }
    if ($code -eq 0) {
        $code = Invoke-Checked 'cargo' @('build', '--release', '--locked', '--target', $Target)
    }
    Pop-Location
    if ($code -ne 0) { Die "构建失败（target=$Target）" }
    Ok '构建完成'
}

function New-AppBundle([string]$Stage, [string]$Bin, [string]$Ver) {
    # macOS .app 结构：Contents/MacOS/<bin> + Info.plist + Resources
    $contents = Join-Path $Stage 'RaceBox 同步.app/Contents'
    $macos    = Join-Path $contents 'MacOS'
    $res      = Join-Path $contents 'Resources'
    New-Item -ItemType Directory -Path $macos -Force | Out-Null
    New-Item -ItemType Directory -Path $res   -Force | Out-Null
    Copy-Item $Bin (Join-Path $macos $BinName) -Force
    $ico = Join-Path $AppDir 'assets/icon256.png'
    if (Test-Path $ico) { Copy-Item $ico (Join-Path $res 'icon256.png') -Force }

    $plist = @"
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>CFBundleName</key><string>RaceBox 同步</string>
  <key>CFBundleDisplayName</key><string>RaceBox 同步</string>
  <key>CFBundleIdentifier</key><string>com.wangcw.racewong</string>
  <key>CFBundleExecutable</key><string>$BinName</string>
  <key>CFBundleVersion</key><string>$Ver</string>
  <key>CFBundleShortVersionString</key><string>$Ver</string>
  <key>CFBundlePackageType</key><string>APPL</string>
  <key>LSMinimumSystemVersion</key><string>11.0</string>
  <key>NSHighResolutionCapable</key><true/>
  <key>NSBluetoothAlwaysUsageDescription</key><string>需要蓝牙权限以连接 RaceBox 设备</string>
</dict>
</plist>
"@
    Write-Utf8File (Join-Path $contents 'Info.plist') $plist
}

function New-Deb([string]$Stage, [string]$Bin, [string]$Ver, [string]$OutFile) {
    $root = "$Stage-deb"
    if (Test-Path $root) { Remove-Item $root -Recurse -Force }
    New-Item -ItemType Directory -Path "$root/DEBIAN" -Force | Out-Null
    New-Item -ItemType Directory -Path "$root/usr/bin" -Force | Out-Null
    New-Item -ItemType Directory -Path "$root/usr/share/doc/$BinName" -Force | Out-Null
    Copy-Item $Bin "$root/usr/bin/$BinName" -Force
    if (Test-Path (Join-Path $AppDir 'README.md')) {
        Copy-Item (Join-Path $AppDir 'README.md') "$root/usr/share/doc/$BinName/README.md" -Force
    }
    $control = @"
Package: $BinName
Version: $Ver
Section: utils
Priority: optional
Architecture: amd64
Maintainer: wangcw <rubygreat@msn.com>
Homepage: https://github.com/redgreat/racewong
Description: RaceBox 同步桌面应用
 通过蓝牙连接 RaceBox Mini S，下载轨迹数据并同步到 PostgreSQL / MQTT，
 上传成功后自动清理设备内存。
"@
    Write-Utf8File "$root/DEBIAN/control" $control

    $code = Invoke-Checked 'dpkg-deb' @('--build', '--root-owner-group', $root, $OutFile)
    if ($code -ne 0) { $code = Invoke-Checked 'dpkg-deb' @('--build', $root, $OutFile) }
    Remove-Item $root -Recurse -Force
    return ($code -eq 0)
}

function New-Package([string]$Target, [string]$Ver) {
    $os    = Get-OsLabel $Target
    $arch  = Get-ArchLabel $Target
    $name  = "racewong-$Ver-$os-$arch"
    $stage = Join-Path $OutDir $name
    if (Test-Path $stage) { Remove-Item $stage -Recurse -Force }
    New-Item -ItemType Directory -Path $stage -Force | Out-Null

    $bin = Join-Path $AppDir "target/$Target/release/$BinName"
    if ($os -eq 'windows') { $bin = "$bin.exe" }
    if (-not (Test-Path $bin)) { Die "找不到构建产物: $bin" }

    if ($os -eq 'macos') {
        New-AppBundle $stage $bin $Ver
    } else {
        $destName = if ($os -eq 'windows') { "$BinName.exe" } else { $BinName }
        Copy-Item $bin (Join-Path $stage $destName) -Force
        if ($os -eq 'windows' -and (Test-Path (Join-Path $AppDir 'assets/icon.ico'))) {
            Copy-Item (Join-Path $AppDir 'assets/icon.ico') $stage -Force
        }
    }
    if (Test-Path (Join-Path $AppDir 'README.md')) {
        Copy-Item (Join-Path $AppDir 'README.md') (Join-Path $stage 'README.txt') -Force
    }
    if (Test-Path 'LICENSE') { Copy-Item 'LICENSE' $stage -Force }

    $assets = @()
    if ($os -eq 'windows') {
        $zip = Join-Path $OutDir "$name.zip"
        if (Test-Path $zip) { Remove-Item $zip -Force }
        Compress-Archive -Path (Join-Path $stage '*') -DestinationPath $zip -Force
        $assets += $zip
    } else {
        $tgz = Join-Path $OutDir "$name.tar.gz"
        if (Test-Path $tgz) { Remove-Item $tgz -Force }
        $code = Invoke-Checked 'tar' @('-czf', $tgz, '-C', $OutDir, $name)
        if ($code -ne 0) { Die '打包 tar.gz 失败' }
        $assets += $tgz

        if ($os -eq 'macos' -and (Get-HostOS) -eq 'macos' -and (Get-Tool 'hdiutil')) {
            $dmg = Join-Path $OutDir "$name.dmg"
            if (Test-Path $dmg) { Remove-Item $dmg -Force }
            $code = Invoke-Checked 'hdiutil' @('create', '-volname', "RaceBox 同步 $Ver", '-srcfolder', $stage, '-ov', '-format', 'UDZO', $dmg)
            if ($code -eq 0) { $assets += $dmg } else { Warn '生成 dmg 失败，仅保留 tar.gz' }
        }
        if ($os -eq 'linux' -and (Get-Tool 'dpkg-deb')) {
            $deb = Join-Path $OutDir "$name.deb"
            if (Test-Path $deb) { Remove-Item $deb -Force }
            if (New-Deb $stage $bin $Ver $deb) { $assets += $deb } else { Warn '生成 deb 失败，仅保留 tar.gz' }
        }
    }

    foreach ($a in $assets) { Ok "产出 $(Split-Path -Leaf $a)" }
    return $assets
}

# ---------------------------------------------------------------- GitHub Release
function Publish-Release([string]$Tag, [string]$Notes, [string[]]$Assets) {
    $notesFile = Join-Path $OutDir 'release-notes.md'
    Write-Utf8File $notesFile $Notes

    $gh = Get-Tool 'gh'
    if ($gh) {
        $exists = $false
        try {
            $null = & gh release view $Tag 2>$null
            $exists = ($LASTEXITCODE -eq 0)
        } catch {
            $exists = $false
        }
        if (-not $exists) {
            Step "创建 GitHub Release $Tag"
            $ghArgs = @('release', 'create', $Tag, '--title', "RaceBox 同步 $Tag", '--notes-file', $notesFile)
            if ($Tag -match '-') { $ghArgs += '--prerelease' }
            if ($DryRun) { Say "   [dry-run] gh $($ghArgs -join ' ')" }
            else {
                $code = Invoke-Checked 'gh' $ghArgs
                if ($code -ne 0) { Die '创建 Release 失败' }
                Ok 'Release 已创建'
            }
        } else {
            Ok "Release $Tag 已存在，直接上传附件"
        }
        if ($Assets.Count -gt 0) {
            Step "上传 $($Assets.Count) 个安装包"
            if ($DryRun) { Say "   [dry-run] gh release upload $Tag $($Assets -join ' ') --clobber" }
            else {
                $code = Invoke-Checked 'gh' (@('release', 'upload', $Tag) + $Assets + @('--clobber'))
                if ($code -ne 0) { Die '上传附件失败' }
                Ok '附件已上传'
            }
        }
        return
    }

    # 没有 gh：用 GITHUB_TOKEN 走 REST
    $token = $env:GITHUB_TOKEN
    if (-not $token) { $token = $env:GH_TOKEN }
    if (-not $token) {
        Warn '未安装 gh 且没有 GITHUB_TOKEN，跳过 Release；说明已写入 dist/release-notes.md'
        return
    }
    $remote = (& git remote get-url origin | Select-Object -First 1)
    if (-not $remote) { Warn '没有 origin 远端，跳过 Release'; return }
    $remote = $remote.Trim()
    if ($remote -notmatch 'github\.com[:/](.+?)(\.git)?$') { Warn "无法解析 GitHub 仓库: $remote"; return }
    $repo = $Matches[1]

    $headers = @{ Authorization = "Bearer $token"; Accept = 'application/vnd.github+json'; 'User-Agent' = 'racewong-release' }
    Step "通过 REST 创建/更新 GitHub Release $Tag"
    $releaseId = $null
    try {
        $r = Invoke-RestMethod -Headers $headers -Method Get -Uri "https://api.github.com/repos/$repo/releases/tags/$Tag"
        $releaseId = $r.id
    } catch {
        $releaseId = $null
    }
    if (-not $releaseId) {
        if ($DryRun) { Say "   [dry-run] POST /repos/$repo/releases" }
        else {
            $payload = @{
                tag_name   = $Tag
                name       = "RaceBox 同步 $Tag"
                body       = $Notes
                draft      = $false
                prerelease = ($Tag -match '-')
            } | ConvertTo-Json
            $r = Invoke-RestMethod -Headers $headers -Method Post -Uri "https://api.github.com/repos/$repo/releases" -Body $payload -ContentType 'application/json'
            $releaseId = $r.id
            Ok 'Release 已创建'
        }
    }
    if ($DryRun -or -not $releaseId) { return }
    foreach ($a in $Assets) {
        $leaf = [Uri]::EscapeDataString((Split-Path -Leaf $a))
        $up = "https://uploads.github.com/repos/$repo/releases/$releaseId/assets?name=$leaf"
        $null = Invoke-RestMethod -Headers $headers -Method Post -ContentType 'application/octet-stream' -InFile $a -Uri $up
        Ok "已上传 $(Split-Path -Leaf $a)"
    }
}

# ---------------------------------------------------------------- 主流程
if (-not (Get-Tool 'git')) { Die '找不到 git' }

# 只输出 Release 正文（CI 的 publish 环节用）
if ($NotesOnly) {
    $toRef = 'HEAD'
    if ($Version) {
        $v = $Version.Trim()
        if ($v -notmatch '^v') { $v = "v$v" }
        $toRef = $v
    }
    $tags = @(& git tag --list 'v*.*.*' --sort=-version:refname)
    $cur  = ''
    try { $cur = (& git describe --tags --exact-match HEAD 2>$null | Select-Object -First 1) } catch { $cur = '' }
    $from = ''
    if ($cur) {
        if ($tags.Count -gt 1) { $from = $tags[1] }
    } elseif ($tags.Count -gt 0) {
        $from = $tags[0]
    }
    Say (Get-ReleaseNotes $from $toRef)
    exit 0
}

# 只构建当前平台
if ($BuildOnly) {
    $ver = Get-CargoVersion
    $target = Get-TargetForHost
    Invoke-Build $target
    $assets = New-Package $target $ver
    Step '完成'
    foreach ($a in $assets) { Say (Resolve-Path $a | Select-Object -ExpandProperty Path) }
    exit 0
}

# ---- 完整发布 ----
Step '检查工作区'
$branch = (& git rev-parse --abbrev-ref HEAD | Select-Object -First 1)
if (-not $branch) { Die '无法获取当前分支' }
$branch = $branch.Trim()
$dirty = (& git status --porcelain)
if ($dirty -and -not $Force) { Die "工作区不干净，请先提交或暂存改动（或用 -Force 忽略）：`n$dirty" }
Ok "分支 $branch"

$null = & git fetch --tags origin 2>$null

$verNoV = ''
if ([string]::IsNullOrWhiteSpace($Version)) {
    $latest = Get-LatestTag
    $verNoV = Get-NextVersion $latest
    Ok "自动版本：$(if ($latest) { "$latest -> v$verNoV" } else { "v$verNoV（无历史标签）" })"
} else {
    $verNoV = $Version.Trim()
    if ($verNoV -match '^v(\d+\.\d+\.\d+.*)$') { $verNoV = $Matches[1] }
    if ($verNoV -notmatch '^\d+\.\d+\.\d+') { Die "版本格式错误: $Version（需要 v主版本.次版本.修订号）" }
}
$tag = "v$verNoV"
if (& git tag --list $tag) { Die "标签 $tag 已存在" }
Ok "目标版本 $tag"

# Release 正文要在打新标签之前算好
$prevTag = Get-LatestTag
$notes   = Get-ReleaseNotes $prevTag 'HEAD'

Step '同步 app/Cargo.toml 版本'
$oldVer = Get-CargoVersion
if ($oldVer -ne $verNoV) {
    if ($DryRun) { Say "   [dry-run] app/Cargo.toml: $oldVer -> $verNoV" }
    else {
        $null = Set-CargoVersion $verNoV
        $code = Invoke-Checked 'git' @('add', (Join-Path $AppDir 'Cargo.toml'))
        if ($code -ne 0) { Die 'git add 失败' }
        $code = Invoke-Checked 'git' @('commit', '-m', "chore(release): bump version to $verNoV")
        if ($code -ne 0) { Die '提交失败' }
        Ok "版本号 $oldVer -> $verNoV"
    }
} else {
    Ok "版本号已是 $verNoV"
}

Step "打标签 $tag"
if ($DryRun) { Say "   [dry-run] git tag -a $tag -m 'Release $tag'" }
else {
    $code = Invoke-Checked 'git' @('tag', '-a', $tag, '-m', "Release $tag")
    if ($code -ne 0) { Die '创建标签失败' }
    Ok '标签已创建'
}

Step '推送提交与标签'
if ($DryRun) { Say "   [dry-run] git push origin $branch && git push origin $tag" }
else {
    $code = Invoke-Checked 'git' @('push', 'origin', $branch)
    if ($code -ne 0) { Warn "推送分支 $branch 失败（可能没有上游），继续推送标签" }
    $code = Invoke-Checked 'git' @('push', 'origin', $tag)
    if ($code -ne 0) { Die '推送标签失败' }
    Ok "已推送 $tag"
}

$assets = @()
if (-not $NoBuild) {
    $target = Get-TargetForHost
    Invoke-Build $target
    $assets = @(New-Package $target $verNoV)
} else {
    Ok '-NoBuild：本机不构建，三个平台的包由 GitHub Actions 产出'
}

Publish-Release $tag $notes $assets

Step '发布完成'
Say "  Release : https://github.com/redgreat/racewong/releases/tag/$tag"
Say "  说明来源: git 提交记录$(if ($prevTag) { "（$prevTag..HEAD）" } else { '（全部历史）' })"
if ($NoBuild -or $assets.Count -eq 0) {
    Say '  GitHub Actions 会构建 win-x86_64 / macos-aarch64 / linux-x86_64 三个包并上传到该 Release'
}
