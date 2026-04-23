$projectDir = "E:\VScode\Chi phí ads"
$logDir = Join-Path $projectDir "storage\logs"
$logPath = Join-Path $logDir "auto_fill.log"

New-Item -ItemType Directory -Force -Path $logDir | Out-Null

$machinePath = [System.Environment]::GetEnvironmentVariable("PATH", "Machine")
$userPath = [System.Environment]::GetEnvironmentVariable("PATH", "User")
$env:PATH = "$machinePath;$userPath"

function Write-Log {
    param([string]$Message)
    $line = "[{0}] {1}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $Message
    Add-Content -Path $logPath -Value $line
}

try {
    $alreadyRunning = Get-CimInstance Win32_Process |
        Where-Object {
            $_.Name -eq "python.exe" -and
            $_.CommandLine -match "fb_ads_tool.py run-auto"
        } |
        Select-Object -First 1

    if ($alreadyRunning) {
        Write-Log "Auto-fill da dang chay (PID=$($alreadyRunning.ProcessId)). Bo qua lan khoi dong nay."
        exit 0
    }

    Set-Location $projectDir
    Write-Log "Khoi dong auto-fill service (hidden startup)."

    python fb_ads_tool.py run-auto *>> $logPath
}
catch {
    Write-Log "Loi startup: $($_.Exception.Message)"
    throw
}
