# MSI 许可页的 RTF 生成器。
#
# WiX 的 `WixUILicenseRtf` 只认 RTF（许可页是 RichText 控件），而 RTF 里的
# 非 ASCII 字符必须写成 `\uNNNN?` 转义——直接把 UTF-8 或 GBK 字节塞进去，
# 中文一定显示成乱码或方框。
#
# 所以协议正文维护在 `app/installer/license.zh.txt`（UTF-8，人读），
# 打包时由 `script/release.ps1` 调这里现转成 RTF。
#
# 单独抽成文件是为了能独立验证：`. .\script\wix-license.ps1` 之后
# 直接 `ConvertTo-LicenseRtf (Get-Content ... -Raw)` 就能看产物。
#
# 注意：本文件必须存成 **UTF-8 带 BOM**。Windows PowerShell 5.1 读没有 BOM 的
# UTF-8 会按 ANSI 解析，中文注释的字节会打乱语法，函数静默地少追加内容（踩过）。

function ConvertTo-LicenseRtf {
    param([Parameter(Mandatory = $true)][string]$Text)

    $sb = New-Object System.Text.StringBuilder
    # ansicpg936 + fcharset134：让 RichEdit 用中文字体渲染，避免回退成方框
    [void]$sb.Append('{\rtf1\ansi\ansicpg936\deff0{\fonttbl{\f0\fnil\fcharset134 Microsoft YaHei;}}' + "`r`n")
    [void]$sb.Append('\f0\fs18 ')
    foreach ($ch in $Text.ToCharArray()) {
        $code = [int]$ch
        if ($code -eq 10) { [void]$sb.Append("\par`r`n"); continue }
        if ($code -eq 13) { continue }
        if ($code -eq 9) { [void]$sb.Append('\tab '); continue }
        if ($code -lt 128) {
            switch -CaseSensitive ([string]$ch) {
                '\' { [void]$sb.Append('\\') }
                '{' { [void]$sb.Append('\{') }
                '}' { [void]$sb.Append('\}') }
                default { [void]$sb.Append($ch) }
            }
            continue
        }
        # RTF 的 \u 参数是**有符号** 16 位，超过 32767 要折成负数
        if ($code -gt 32767) { $code -= 65536 }
        # 必须分段拼：`"\u$code?"` 会被 PowerShell 当成名为 `code?` 的变量，
        # 展开成空字符串，转义序列就只剩一个 `\u`（踩过）。
        [void]$sb.Append('\u').Append($code).Append('?')
    }
    [void]$sb.Append('}')
    return $sb.ToString()
}
