<#
upload_sql_sftp.ps1 for ExtractorDeEdC_With-CEP
Uploads extractor_estados.sql to remote host via scp/pscp. Prefills host/user from local .env if present.
#>
param(
    [string] $LocalFile = "extractor_estados.sql"
)

function Read-EnvValue($key) {
    $envPath = Join-Path $PSScriptRoot "..\.env"
    if (Test-Path $envPath) {
        $lines = Get-Content $envPath | Where-Object { $_ -match '=' }
        foreach ($l in $lines) {
            $parts = $l -split '=', 2
            if ($parts[0].Trim() -eq $key) { return $parts[1].Trim() }
        }
    }
    return $null
}

$LocalFile = Resolve-Path -Path $LocalFile -ErrorAction SilentlyContinue
if (-not $LocalFile) { Write-Error "Archivo local no encontrado: $LocalFile"; exit 1 }

$defaultHost = Read-EnvValue 'DB_HOST'
$defaultUser = Read-EnvValue 'DB_USER'

$RemoteHost = Read-Host -Prompt "Remote host (ej. srv1505.hstgr.io) [default: $defaultHost]"
if ([string]::IsNullOrWhiteSpace($RemoteHost)) { $RemoteHost = $defaultHost }
$RemoteUser = Read-Host -Prompt "Remote user (default: $defaultUser)"
if ([string]::IsNullOrWhiteSpace($RemoteUser)) { $RemoteUser = $defaultUser }
$RemotePath = Read-Host -Prompt 'Remote path (ej. /home/usuario/uploads/ or public_html/)' ; if ([string]::IsNullOrWhiteSpace($RemotePath)) { $RemotePath = '.' }
$Port = Read-Host -Prompt 'SSH port (default 22)'; if ([string]::IsNullOrWhiteSpace($Port)) { $Port = 22 }

$scp = Get-Command scp -ErrorAction SilentlyContinue
if (-not $scp) { $scp = Get-Command pscp.exe -ErrorAction SilentlyContinue }
if (-not $scp) { Write-Error "scp/pscp no encontrado en PATH. Instala OpenSSH o usa WinSCP."; exit 2 }

$scpCmd = $scp.Source
$local = $LocalFile.ProviderPath
$remoteTarget = "${RemoteUser}@${RemoteHost}:$RemotePath"
if ($scpCmd -like '*pscp*') {
    # pscp uses -P for port as well
    & $scpCmd -P $Port "$local" "$remoteTarget"
} else {
    & $scpCmd -P $Port "$local" "$remoteTarget"
}
if ($LASTEXITCODE -eq 0) { Write-Host "Upload completado." -ForegroundColor Green } else { Write-Error "scp falló con código $LASTEXITCODE"; exit $LASTEXITCODE }
