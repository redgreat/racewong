#!/usr/bin/env pwsh
#Requires -Version 5.1
# racewong 客户端本地编译脚本（Windows 原生）
#
# 用法：
#   .\script\build.ps1                # release 构建
#   .\script\build.ps1 -Debug         # debug 构建（带控制台，方便看日志）
#   .\script\build.ps1 -Run           # 构建完启动客户端
#   .\script\build.ps1 -Check         # 构建完跑 --check 自检
#   .\script\build.ps1 -Test          # 编译前先跑协议单测
#   .\script\build.ps1 -Clean         # 编译前 cargo clean
#   .\script\build.ps1 -NoWsl         # 本机没 cargo 时不要自动转到 WSL
#   .\script\build.ps1 -NoKill        # 不要自动结束还在跑的客户端（默认会自动结束）
#
# 说明：
#   - 本机有 Rust 工具链时直接 cargo build（Windows 原生，产物带中文版本信息）。
#   - 本机没有 cargo 但装了 WSL 时，自动转到 WSL 里跑 script/build.sh 交叉编译
#     （产物仍是 Windows exe，见 app/README.md「本地（Windows）构建」）。
#   - Windows 上正在运行的 exe 无法被覆盖，cargo 写产物时会报
#     "failed to remove file ... os error 5"。所以编译前会先结束还在跑的客户端
#     （尤其是启用了「隐藏到托盘」后关窗口不退进程的情况）。

param(
    [switch]$Debug,
    [switch]$Run,
    [switch]$Check,
    [switch]$Test,
    [switch]$Clean,
    [switch]$NoWsl,
    [switch]$NoKill
)

# 与 release.ps1 一致：不让 PowerShell 7 把外部命令的 stderr 包装成终止性错误
$ErrorActionPreference = 'Continue'
if (Get-Variable -Name PSNativeCommandUseErrorActionPreference -ErrorAction SilentlyContinue) {
    $PSNativeCommandUseErrorActionPreference = $false
}
Set-Location (Split-Path -Parent $PSScriptRoot)

$AppDir  = 'app'
$BinName = 'racewong-app'

function Say($m)  { Write-Host $m }
function Step($m) { Write-Host "==> $m" -ForegroundColor Cyan }
function Ok($m)   { Write-Host "   $m" -ForegroundColor Green }
function Warn($m) { Write-Host "!  $m" -ForegroundColor Yellow }
function Die($m)  { Write-Host "错误: $m" -ForegroundColor Red; exit 1 }

function ConvertTo-WslPath([string]$Path) {
    if ($Path -match '^([A-Za-z]):[\\/]?(.*)$') {
        $drive = $Matches[1].ToLower()
        $rest  = $Matches[2] -replace '\\', '/'
        if ($rest) { return "/mnt/$drive/$rest" }
        return "/mnt/$drive"
    }
    return ($Path -replace '\\', '/')
}

# ---------------------------------------------------------------- 结束占用进程
# Windows 上正在运行的 exe 无法被删除/覆盖，rustc 写产物时报 os error 5。
function Get-RunningApp {
    return @(Get-Process -Name $BinName -ErrorAction SilentlyContinue)
}

function Stop-RunningApp {
    $procs = Get-RunningApp
    if ($procs.Count -eq 0) { return }

    $pids = ($procs | ForEach-Object { $_.Id }) -join ', '
    if ($NoKill) {
        Warn "检测到 $BinName.exe 正在运行（PID $pids），编译时可能报 os error 5"
        Warn '先在托盘菜单里点「退出」，或去掉 -NoKill 让本脚本自动结束它'
        return
    }

    Warn "检测到 $BinName.exe 正在运行（PID $pids），先结束它再编译"
    foreach ($p in $procs) {
        try { $p.Kill(); $p.WaitForExit(5000) | Out-Null } catch { }
    }
    # 等 Windows 把文件句柄真正释放掉
    for ($i = 0; $i -lt 20; $i++) {
        if ((Get-RunningApp).Count -eq 0) { break }
        Start-Sleep -Milliseconds 150
    }
    if ((Get-RunningApp).Count -gt 0) {
        Warn '结束进程失败（可能权限不足）；先在托盘里退出客户端，或用任务管理器结束 racewong-app.exe'
        return
    }
    Ok '已结束旧进程'
}

function Hint-IfLocked {
    if ($LASTEXITCODE -eq 0) { return }
    if ((Get-RunningApp).Count -gt 0) {
        Warn "$BinName.exe 还在运行，产物无法覆盖（os error 5）"
        Warn '先在托盘菜单里点「退出」再重试，或去掉 -NoKill 让本脚本自动结束它'
    }
}

# ---------------------------------------------------------------- 找 cargo
$cargo = $null
$cmd = Get-Command cargo -ErrorAction SilentlyContinue
if ($cmd) { $cargo = $cmd.Source }
if (-not $cargo) {
    $candidates = @()
    if ($env:CARGO_HOME) { $candidates += (Join-Path $env:CARGO_HOME 'bin\cargo.exe') }
    if ($env:USERPROFILE) { $candidates += (Join-Path $env:USERPROFILE '.cargo\bin\cargo.exe') }
    foreach ($c in $candidates) {
        if (Test-Path $c) { $cargo = $c; break }
    }
}

# 本机没工具链：有 WSL 就转过去交叉编译，省得用户自己拼命令
if (-not $cargo -and -not $NoWsl) {
    $wsl = Get-Command wsl.exe -ErrorAction SilentlyContinue
    if ($wsl) {
        $wslRoot = ConvertTo-WslPath (Get-Location).Path
        $flags = @()
        if ($Debug) { $flags += '--debug' } else { $flags += '--release' }
        if ($Run)   { $flags += '--run' }
        if ($Check) { $flags += '--check' }
        if ($Test)  { $flags += '--test' }
        if ($Clean) { $flags += '--clean' }
        if ($NoKill) { $flags += '--no-kill' }
        $line = "cd '$wslRoot' && ./script/build.sh " + ($flags -join ' ')
        # WSL 侧的 taskkill 走的是 Windows 互操作，可能被关掉；这里先本机结束一次更稳
        Stop-RunningApp
        Warn '本机没有 cargo，转到 WSL 里用 script/build.sh 交叉编译'
        Ok "wsl -e bash -lc `"$line`""
        & wsl.exe -e bash -lc $line
        exit $LASTEXITCODE
    }
}

if (-not $cargo) {
    Die '本机找不到 cargo。装 Rust 工具链（https://rustup.rs，或 winget install Rustlang.Rustup）后重试；也可以在 WSL 里跑 ./script/build.sh。'
}

# ---------------------------------------------------------------- 编译
$profileArgs = @()
if (-not $Debug) { $profileArgs += '--release' }

# cargo clean 删 target 里的旧 exe 时同样会被占用，所以放在 clean 之前
Step '检查是否还有客户端在运行'
Stop-RunningApp

Push-Location $AppDir
try {
    if ($Clean) {
        Step '清理旧产物'
        & $cargo clean
        Hint-IfLocked
        if ($LASTEXITCODE -ne 0) { Die 'cargo clean 失败' }
    }
    if ($Test) {
        Step '协议单测'
        & $cargo test --bins
        if ($LASTEXITCODE -ne 0) { Die '单元测试失败' }
    }
    $profileName = if ($Debug) { 'debug' } else { 'release' }
    Step "编译（$profileName）"
    & $cargo build @profileArgs
    Hint-IfLocked
    if ($LASTEXITCODE -ne 0) { Die '编译失败' }
}
finally {
    Pop-Location
}

# ---------------------------------------------------------------- 产物
$binRel = Join-Path (Join-Path 'target' $profileName) "$BinName.exe"
$bin = Join-Path $AppDir $binRel
if (-not (Test-Path $bin)) { Die "编译完成但没找到产物: $bin" }
$bin = (Resolve-Path $bin).Path
$sizeMb = [math]::Round((Get-Item $bin).Length / 1MB, 1)

Step '产物'
Ok "$bin（$sizeMb MB）"

if ($Check) {
    Step '连通性自检（--check）'
    if (-not $Debug) {
        Warn 'release 版是 GUI 子系统，PowerShell 里可能看不到 stdout；看不到就用 -Debug 构建后再跑'
    }
    & $bin --check
}

if ($Run) {
    Step '启动客户端'
    Start-Process -FilePath $bin
    Ok '已启动'
}
