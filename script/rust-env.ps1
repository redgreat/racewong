# 本机 Rust 编译环境（一次性设置，只对当前 PowerShell 会话生效）
#
# 为什么需要它：这台机器上没装完整的 "C++ build tools"，rustup 装的
# x86_64-pc-windows-msvc 目标找不到 MSVC 的 link.exe，cargo 会在编
# build script 时就报
#     error: linking with `link.exe` failed: exit code: 1
#     note: in the Visual Studio installer, ensure the "C++ build tools" workload is selected
#
# 但 VS Community 里带了一套自用的 C++ 工具链（ScopeCppSDK/vc15），里面
# cl.exe / link.exe / rc.exe 和全套 lib、include 都有，版本是 14.12（VS2017）。
# 用它能够完整编出 exe（实测 cargo build / cargo test 都通过）。
#
# 用法（在仓库根目录）：
#     . .\script\rust-env.ps1
#     cd app; cargo check --all-targets; cargo test
#
# 注意：Git for Windows 的 /usr/bin/link.exe 会顶掉 MSVC 的 link.exe
# （报 "link: extra operand" 就是它干的好事），所以这里把工具链目录放在 PATH 最前面。

$sdk = "D:\Program Files\Microsoft Visual Studio\18\Community\SDK\ScopeCppSDK\vc15"

$env:PATH    = "$sdk\VC\bin;$sdk\SDK\bin;$env:USERPROFILE\.cargo\bin;$env:PATH"
$env:LIB     = "$sdk\VC\lib;$sdk\SDK\lib"
$env:INCLUDE = "$sdk\VC\include;$sdk\SDK\include\ucrt;$sdk\SDK\include\um;$sdk\SDK\include\shared"

if (Get-Command cargo -ErrorAction SilentlyContinue) {
    Write-Host "rust 环境就绪: $((cargo --version))  /  $((rustc --version))"
} else {
    Write-Host "没找到 cargo：先跑 rustup-init.exe 装一次（默认 toolchain 即可）" -ForegroundColor Yellow
}
