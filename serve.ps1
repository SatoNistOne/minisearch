param([string]$Command = "")

[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
Set-Location $PSScriptRoot
$port = 7000
New-Item -ItemType Directory -Force target | Out-Null
$log = "target\serve.log"

function Stop-Site {
    Write-Host "Останавливаю сайт…"
    cmd /c "docker compose stop >> $log 2>&1"
    Write-Host "Сайт остановлен."
}

if ($Command -eq "stop") {
    Stop-Site
    exit 0
}

Set-Content $log ""
Write-Host "Запуск…"
cmd /c "docker compose up -d --build >> $log 2>&1"
if ($LASTEXITCODE -ne 0) {
    Get-Content $log -Tail 20
    exit 1
}

$state = ""
for ($i = 0; $i -lt 120; $i++) {
    $id = docker compose ps -q coordinator 2>$null
    if ($id) { $state = docker inspect -f "{{.State.Health.Status}}" $id 2>$null }
    if ($state -eq "healthy") { break }
    Start-Sleep -Seconds 1
}
if ($state -ne "healthy") {
    Write-Host "Сайт не запустился, подробности в $log"
    Stop-Site
    exit 1
}

$addresses = Get-NetIPConfiguration |
    Where-Object { $_.IPv4DefaultGateway -and $_.NetAdapter.Status -eq "Up" } |
    ForEach-Object { $_.IPv4Address.IPAddress }

Write-Host ""
Write-Host "Сайт работает:"
Write-Host "  на этом компьютере:   http://127.0.0.1:$port"
if ($addresses) {
    foreach ($addr in $addresses) { Write-Host "  с других устройств:  http://${addr}:$port" }
} else {
    Write-Host "  с других устройств:  нет подключения к сети"
}
Write-Host ""
Write-Host "Ctrl+C — остановить сайт"
try {
    while ($true) { Start-Sleep -Seconds 1 }
} finally {
    Stop-Site
}
