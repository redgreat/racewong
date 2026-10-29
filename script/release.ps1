#!/usr/bin/env pwsh
#Requires -Version 5.1
# racewong 发布脚本（兼容 Windows PowerShell 5.1 与 PowerShell 7+）
#
# 完整发布流程：
#   读取/递增版本 -> 同步 app/Cargo.toml -> 提交 -> 打标签 -> 推送
#   -> 用 git 提交记录生成 Release 说明 -> 创建/更新 GitHub Release -> 上传安装包
#
# 三个平台的安装包（本脚本只构建"当前这台机器"能构建的那一个，其余交给 GitHub Actions）：
#   Windows x86-64 : racewong-<ver>-windows-x86_64.msi
#   macOS   arm64  : racewong-<ver>-macos-aarch64.dmg（CI 的 macOS runner 生成）
#   Linux   x86-64 : racewong-<ver>-linux-x86_64.tar.gz（有 dpkg-deb 时还会出 .deb）
#
# 用法：
#   .\script\release.ps1                      # 自动 patch +1，走完整流程
#   .\script\release.ps1 -Version v1.2.0      # 指定版本
#   .\script\release.ps1 -Version 1.2.0       # 也可以不带 v
#   .\script\release.ps1 -NoBuild             # 只打标签 + 建 Release，包全交给 CI
#   .\script\release.ps1 -Build               # 强制在本机构建当前平台的包（即使没地方上传）
#   .\script\release.ps1 -BuildOnly           # 只构建当前平台安装包（CI 用），不做 git/gh 操作
#   .\script\release.ps1 -NotesOnly           # 只输出 git 提交记录（当 Release 正文）
#   .\script\release.ps1 -SkipTests -DryRun   # 演示：跳过测试、只打印不执行
#
# 关于「最后为什么还要构建」：
#   标签推上去之后 GitHub Actions 会把 win/mac/linux 三个包都编好并上传，
#   所以本机构建只在「本机能上传（gh 已登录 / 有 GITHUB_TOKEN）」时才有意义
#   —— 它只是让当前平台的包早几分钟出现在 Release 里。
#   本机没有凭据时脚本会自动跳过这一步（产物无处上传，纯浪费）；
#   本机没有 cargo 时回退到 WSL 里的 script/build.sh。
#   想强制本机构建就加 -Build。
#
# 需要：git、cargo（构建时）、gh（建 Release 时；没装也可以用 GITHUB_TOKEN 走 REST）

param(
    [Parameter(Position = 0)][string]$Version = '',
    [switch]$BuildOnly,
    [switch]$Build,
    [switch]$NoBuild,
    [switch]$NotesOnly,
    [switch]$SkipTests,
    [switch]$Force,
    [switch]$DryRun,
    [string]$OutDir = 'dist'
)

# 这里刻意不用 Stop：PowerShell 7 会把外部命令写到 stderr 的内容（git 的进度信息、
# cargo 的 warning）包装成 ErrorRecord，配合 ErrorActionPreference=Stop 会直接中断脚本。
# 退出码统一由 Invoke-Cmd / Invoke-Checked 用 $LASTEXITCODE 判断。
$ErrorActionPreference = 'Continue'
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

# D:\a\b -> /mnt/d/a/b，给 WSL 回退用
function ConvertTo-WslPath([string]$Path) {
    if ($Path -match '^([A-Za-z]):[\\/]?(.*)$') {
        $drive = $Matches[1].ToLower()
        $rest  = $Matches[2] -replace '\\', '/'
        if ($rest) { return "/mnt/$drive/$rest" }
        return "/mnt/$drive"
    }
    return ($Path -replace '\\', '/')
}

# 直接执行并把输出留给终端（构建这类耗时命令用）。
# 注意：必须把输出吞掉，否则函数的返回值会变成「输出 + 退出码」的数组，
# 调用方用 -ne 0 判断时会误判成失败（cargo 一有输出就"构建失败"）。
function Invoke-Checked([string]$Exe, [string[]]$ExeArgs) {
    & $Exe @ExeArgs 2>&1 | Out-Host
    return $LASTEXITCODE
}

# 执行并捕获输出：失败时能把真实原因（stderr）显示出来。
# 返回 @{ Code = <退出码>; Output = <输出文本> }
function Invoke-Cmd([string]$Exe, [string[]]$ExeArgs) {
    $output = & $Exe @ExeArgs 2>&1
    $code = $LASTEXITCODE
    $text = ''
    if ($null -ne $output) {
        # stderr 被合并进来的是 ErrorRecord，取它的文本，避免显示成一长串 PowerShell 报错
        $text = (($output | ForEach-Object { $_.ToString() }) -join "`n").Trim()
    }
    return @{ Code = $code; Output = $text }
}

# 执行 git 并检查退出码，失败时带上 git 的真实输出。
function Invoke-Git([string[]]$GitArgs) {
    $r = Invoke-Cmd 'git' $GitArgs
    if ($r.Code -ne 0) {
        $detail = if ($r.Output) { "`n$($r.Output)" } else { '' }
        Die "git $($GitArgs -join ' ') 失败（退出码 $($r.Code)）$detail"
    }
    return $r.Output
}

# 前置检查：提交身份、origin 远端，避免走到一半才炸。
function Test-GitReady {
    $name  = (Invoke-Cmd 'git' @('config', 'user.name')).Output
    $email = (Invoke-Cmd 'git' @('config', 'user.email')).Output
    if (-not $name -or -not $email) {
        Die 'git 未配置提交身份，请先执行：git config --global user.name "你的名字" ; git config --global user.email "you@example.com"'
    }
    $remotes = (Invoke-Cmd 'git' @('remote')).Output
    if ($remotes -notmatch '(^|\s)origin(\s|$)') { Die '没有 origin 远端，无法推送标签' }
    Ok "提交身份 $name <$email>"
}

# 远端是否已有某个标签（用于"本地有、远端没有"时补推）。
function Test-RemoteTag([string]$Tag) {
    $r = Invoke-Cmd 'git' @('ls-remote', '--tags', 'origin', "refs/tags/$Tag")
    if ($r.Code -ne 0) { return $false }
    return -not [string]::IsNullOrWhiteSpace($r.Output)
}

# GitHub 上是否已有该 tag 的 Release（gh 可用时才判断）。
function Test-ReleaseExists([string]$Tag) {
    if (-not (Get-Tool 'gh')) { return $false }
    $r = Invoke-Cmd 'gh' @('release', 'view', $Tag)
    return ($r.Code -eq 0)
}

# gh 是否已登录；没登录就交给 CI 去建 Release。
function Test-GhAuth {
    if (-not (Get-Tool 'gh')) { return $false }
    $r = Invoke-Cmd 'gh' @('auth', 'status')
    return ($r.Code -eq 0)
}

# 本机有没有把安装包传上去的手段（gh 已登录，或设了 token）。
# 没有的话本机构建出来的包无处可去 —— 标签推上去后 CI 会把三个平台都编好。
function Test-CanPublish {
    if (Test-GhAuth) { return $true }
    if ($env:GITHUB_TOKEN -or $env:GH_TOKEN) { return $true }
    return $false
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
    $r = Invoke-Cmd 'git' @('tag', '--list', 'v*.*.*', '--sort=-version:refname')
    if ($r.Code -eq 0 -and $r.Output) {
        $first = $r.Output -split "`r?`n" | Where-Object { $_ } | Select-Object -First 1
        if ($first) { return $first.Trim() }
    }
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
    $r = Invoke-Cmd 'git' @('log', '--no-merges', "--pretty=format:* %s (%h)", $range)
    $lines = @()
    if ($r.Code -eq 0 -and $r.Output) {
        $lines = @($r.Output -split "`r?`n" | Where-Object { $_ })
    }
    if ($lines.Count -eq 0) { return "本次发布没有新的提交记录。" }
    $head = if ($From) { "## 变更记录（$From .. $To）" } else { "## 变更记录" }
    return ($head + "`n" + ($lines -join "`n"))
}

# ---------------------------------------------------------------- 构建与打包
function Invoke-Build([string]$Target) {
    Step "构建 $Target"

    # 本机没有工具链时，回退到 WSL 里的 script/build.sh 交叉编译（和 build.ps1 同一条路）。
    # 产物落在同一个 app/target/<target>/release/ 下，后面的打包逻辑不用改。
    if (-not (Get-Tool 'cargo')) {
        $wsl = Get-Tool 'wsl.exe'
        if ($wsl) {
            $wslRoot = ConvertTo-WslPath $RepoRoot
            $flags = @('--release', '--target', $Target)
            if (-not $SkipTests) { $flags += '--test' }
            $line = "cd '$wslRoot' && ./script/build.sh " + ($flags -join ' ')
            Warn '本机没有 cargo，转到 WSL 里用 script/build.sh 交叉编译'
            Ok "wsl -e bash -lc `"$line`""
            & wsl.exe -e bash -lc $line
            if ($LASTEXITCODE -ne 0) { Die "构建失败（target=$Target，WSL）" }
            Ok '构建完成'
            return
        }
        Die '找不到 cargo，请先安装 Rust 工具链；或者加 -NoBuild 让 GitHub Actions 构建（推荐）'
    }

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

# 把暂存目录打成 zip。
function New-Zip([string]$Stage, [string]$Name) {
    $zip = Join-Path $OutDir "$Name.zip"
    if (Test-Path $zip) { Remove-Item $zip -Force }
    Compress-Archive -Path (Join-Path $Stage '*') -DestinationPath $zip -Force
    Ok "产出 $Name.zip"
    return $zip
}

# 用 WiX Toolset 3 生成 MSI；安装包是发布硬要求，不再退回单 exe/zip。
function New-Msi([string]$Stage, [string]$Bin, [string]$Ver, [string]$Name) {
    $candle = Get-Tool 'candle.exe'
    $light  = Get-Tool 'light.exe'
    if (-not $candle -or -not $light) {
        Die '没有 WiX Toolset 3（candle.exe / light.exe），无法生成 MSI'
    }
    $wxs = Join-Path $RepoRoot 'app/installer/racewong.wxs'
    if (-not (Test-Path $wxs)) {
        Die "缺少 $wxs，无法生成 MSI"
    }
    $wixObj  = Join-Path $Stage 'racewong.wixobj'
    $exeAbs  = (Resolve-Path $Bin).Path
    $iconAbs = (Resolve-Path (Join-Path $AppDir 'assets/icon.ico')).Path
    $code = Invoke-Checked $candle.Source @('-nologo', "-dVersion=$Ver", "-dExePath=$exeAbs", "-dIconPath=$iconAbs", '-out', $wixObj, $wxs)
    if ($code -ne 0) { Die 'WiX candle 编译失败' }
    $msi = Join-Path $OutDir "$Name.msi"
    if (Test-Path $msi) { Remove-Item $msi -Force }
    $code = Invoke-Checked $light.Source @('-nologo', '-sval', '-out', $msi, $wixObj)
    if ($code -ne 0) { Die 'WiX light 生成 MSI 失败' }
    Ok "产出 $Name.msi"
    return $msi
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

    # macOS 也不做 .app / dmg，直接给可执行文件（未签名，避免 Gatekeeper 拦截）
    $destName = if ($os -eq 'windows') { "$BinName.exe" } else { $BinName }
    Copy-Item $bin (Join-Path $stage $destName) -Force
    if ($os -eq 'windows' -and (Test-Path (Join-Path $AppDir 'assets/icon.ico'))) {
        Copy-Item (Join-Path $AppDir 'assets/icon.ico') $stage -Force
    }
    if (Test-Path (Join-Path $AppDir 'README.md')) {
        Copy-Item (Join-Path $AppDir 'README.md') (Join-Path $stage 'README.txt') -Force
    }
    if (Test-Path 'LICENSE') { Copy-Item 'LICENSE' $stage -Force }

    $assets = @()
    if ($os -eq 'windows') {
        # Windows Release 只接受 MSI，生成失败就让 CI 明确失败。
        $msi = New-Msi $stage $bin $Ver $name
        $assets += $msi
    } else {
        $tgz = Join-Path $OutDir "$name.tar.gz"
        if (Test-Path $tgz) { Remove-Item $tgz -Force }
        $code = Invoke-Checked 'tar' @('-czf', $tgz, '-C', $OutDir, $name)
        if ($code -ne 0) { Die '打包 tar.gz 失败' }
        $assets += $tgz

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
                $r = Invoke-Cmd 'gh' $ghArgs
                if ($r.Code -ne 0) {
                    # 标签已经推上去了，CI 的 publish 作业会用 GITHUB_TOKEN 建 Release，这里不中断
                    Warn "创建 Release 失败：$($r.Output)"
                    Warn '标签已推送，GitHub Actions 会自动创建 Release 并上传三个平台的包'
                    return
                }
                Ok 'Release 已创建'
            }
        } else {
            Ok "Release $Tag 已存在，直接上传附件"
        }
        if ($Assets.Count -gt 0) {
            Step "上传 $($Assets.Count) 个安装包"
            if ($DryRun) { Say "   [dry-run] gh release upload $Tag $($Assets -join ' ') --clobber" }
            else {
                $r = Invoke-Cmd 'gh' (@('release', 'upload', $Tag) + $Assets + @('--clobber'))
                if ($r.Code -ne 0) { Warn "上传附件失败：$($r.Output)" } else { Ok '附件已上传' }
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
    $remote = (Invoke-Cmd 'git' @('remote', 'get-url', 'origin')).Output.Trim()
    if (-not $remote) { Warn '没有 origin 远端，跳过 Release'; return }
    $remote = $remote.Trim()
    if ($remote -notmatch 'github\.com[:/](.+?)(\.git)?$') { Warn "无法解析 GitHub 仓库: $remote"; return }
    $repo = $Matches[1]

    $headers = @{ Authorization = "Bearer $token"; Accept = 'application/vnd.github+json'; 'User-Agent' = 'racewong-release' }
    Step "通过 REST 创建/更新 GitHub Release $Tag"
    $releaseId = $null
    try {
        $r = Invoke-RestMethod -ErrorAction Stop -Headers $headers -Method Get -Uri "https://api.github.com/repos/$repo/releases/tags/$Tag"
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
            $r = Invoke-RestMethod -ErrorAction Stop -Headers $headers -Method Post -Uri "https://api.github.com/repos/$repo/releases" -Body $payload -ContentType 'application/json'
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
    $tagOut = (Invoke-Cmd 'git' @('tag', '--list', 'v*.*.*', '--sort=-version:refname')).Output
    $tags = @()
    if ($tagOut) { $tags = @($tagOut -split "`r?`n" | Where-Object { $_ }) }
    $cur  = (Invoke-Cmd 'git' @('describe', '--tags', '--exact-match', 'HEAD')).Output.Trim()
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
Test-GitReady
if (-not (Test-GhAuth)) {
    if ($env:GITHUB_TOKEN -or $env:GH_TOKEN) {
        Warn 'gh 未登录，但检测到 GITHUB_TOKEN：Release 走 REST 接口创建'
    } else {
        Warn 'gh 未登录（`gh auth login`）：Release 与安装包交给 GitHub Actions 创建，本机将跳过构建'
    }
}
$branch = (Invoke-Git @('rev-parse', '--abbrev-ref', 'HEAD')).Trim()
if (-not $branch) { Die '无法获取当前分支' }
$dirty = Invoke-Git @('status', '--porcelain')
if ($dirty -and -not $Force) { Die "工作区不干净，请先提交或暂存改动（或用 -Force 忽略）：`n$dirty" }
Ok "分支 $branch"

# 拉远端标签，避免本地残留标签把版本号顶得越来越高
$null = Invoke-Cmd 'git' @('fetch', '--tags', 'origin')

$verNoV = ''
if ([string]::IsNullOrWhiteSpace($Version)) {
    $latest = Get-LatestTag
    if ($latest -and -not (Test-RemoteTag $latest)) {
        Warn "本地标签 $latest 在远端不存在（多半是上次没推成功），仍以它为基准"
    }
    $verNoV = Get-NextVersion $latest
    Ok "自动版本：$(if ($latest) { "$latest -> v$verNoV" } else { "v$verNoV（无历史标签）" })"
} else {
    $verNoV = $Version.Trim()
    if ($verNoV -match '^v(\d+\.\d+\.\d+.*)$') { $verNoV = $Matches[1] }
    if ($verNoV -notmatch '^\d+\.\d+\.\d+') { Die "版本格式错误: $Version（需要 v主版本.次版本.修订号）" }
}
$tag = "v$verNoV"

# 标签可能的状态：本地有 / 远端有。远端已有时不再重复发，只补建 Release
$tagLocal  = -not [string]::IsNullOrWhiteSpace((Invoke-Git @('tag', '--list', $tag)))
$tagRemote = Test-RemoteTag $tag
$skipTag   = $false
if ($tagRemote) {
    if (Test-ReleaseExists $tag) { Die "标签 $tag 与对应 Release 都已存在，换个版本号（或先删远端标签）后再发" }
    Warn "标签 $tag 已在远端（上次可能没创建 Release），跳过打标签与推送，只补建 Release"
    $skipTag = $true
} elseif ($tagLocal) {
    Ok "本地已有标签 $tag（远端没有），跳过创建、稍后补推"
} else {
    Ok "目标版本 $tag"
}

# Release 正文要在打新标签之前算好
$prevTag = Get-LatestTag
$notes   = Get-ReleaseNotes $prevTag 'HEAD'

Step '同步 app/Cargo.toml 版本'
$oldVer = Get-CargoVersion
if ($oldVer -ne $verNoV) {
    if ($DryRun) { Say "   [dry-run] app/Cargo.toml: $oldVer -> $verNoV" }
    else {
        $null = Set-CargoVersion $verNoV
        Invoke-Git @('add', (Join-Path $AppDir 'Cargo.toml'))
        $r = Invoke-Cmd 'git' @('commit', '-m', "chore(release): bump version to $verNoV")
        if ($r.Code -ne 0) {
            if ($r.Output -match 'nothing to commit') { Ok '没有需要提交的改动' }
            else { Die "提交失败：$($r.Output)" }
        } else {
            Ok "版本号 $oldVer -> $verNoV"
        }
    }
} else {
    Ok "版本号已是 $verNoV"
}

Step "打标签 $tag"
if ($skipTag) {
    Ok '标签已在远端，跳过创建'
} elseif ($tagLocal) {
    Ok '标签已存在，跳过创建'
} elseif ($DryRun) {
    Say "   [dry-run] git tag -a $tag -m 'Release $tag'"
} else {
    Invoke-Git @('tag', '-a', $tag, '-m', "Release $tag")
    Ok '标签已创建'
}

Step '推送提交与标签'
if ($DryRun) { Say "   [dry-run] git push origin $branch && git push origin $tag" }
else {
    $r = Invoke-Cmd 'git' @('push', 'origin', $branch)
    if ($r.Code -ne 0) { Warn "推送分支 $branch 失败：$($r.Output)" }
    if ($skipTag) {
        Ok '标签已在远端，跳过推送'
    } else {
        Invoke-Git @('push', 'origin', $tag)
        Ok "已推送 $tag"
    }
}

$assets = @()
if ($NoBuild) {
    Ok '-NoBuild：本机不构建，三个平台的包由 GitHub Actions 产出'
} elseif (-not $Build -and -not (Test-CanPublish)) {
    # 标签已经推上去了，CI 会编好三个平台；本机编出来的包没地方上传，跳过
    Warn '本机没有可用的 GitHub 凭据（gh 未登录、也没设 GITHUB_TOKEN），本地编出来的包无处上传'
    Ok '标签已推送，三个平台的包由 GitHub Actions 构建并上传，本机跳过构建'
    Ok '要强制在本机构建（只想留一份本地 zip 之类），加 -Build'
} else {
    $target = Get-TargetForHost
    Invoke-Build $target
    $assets = @(New-Package $target $verNoV)
}

Publish-Release $tag $notes $assets

Step '发布完成'
Say "  Release : https://github.com/redgreat/racewong/releases/tag/$tag"
Say "  说明来源: git 提交记录$(if ($prevTag) { "（$prevTag..HEAD）" } else { '（全部历史）' })"
if ($NoBuild -or $assets.Count -eq 0) {
    Say '  GitHub Actions 会构建 win-x86_64 / macos-aarch64 / linux-x86_64 三个包并上传到该 Release'
}
