<#
upload_sql_ftp.ps1 for ExtractorDeEdC_With-CEP
Uploads extractor_estados.sql to remote FTP server.
#>
param(
    [string] $LocalFile = "extractor_estados.sql"
)

$LocalFile = Resolve-Path -Path $LocalFile -ErrorAction SilentlyContinue
if (-not $LocalFile) { Write-Error "Archivo local no encontrado: $LocalFile"; exit 1 }

$defaultHost = $null
$envPath = Join-Path $PSScriptRoot "..\.env"
if (Test-Path $envPath) {
    $lines = Get-Content $envPath | Where-Object { $_ -match '=' }
    foreach ($l in $lines) {
        $parts = $l -split '=', 2
        if ($parts[0].Trim() -eq 'DB_HOST') { $defaultHost = $parts[1].Trim() }
    }
}
$ftpHost = Read-Host -Prompt "FTP host (ej. ftp.tudominio.com) [default: $defaultHost]"; if ([string]::IsNullOrWhiteSpace($ftpHost)) { $ftpHost = $defaultHost }
$ftpUser = Read-Host -Prompt 'FTP user'
$ftpPass = Read-Host -AsSecureString 'FTP password' | ConvertFrom-SecureString
$remotePath = Read-Host -Prompt 'Remote path (ej. public_html/uploads/)' ; if ([string]::IsNullOrWhiteSpace($remotePath)) { $remotePath = '.' }

$uri = "ftp://$ftpHost/$remotePath/$(Split-Path -Leaf $LocalFile)"
$plain = Read-Host -Prompt 'Repita la contraseña en texto (se usará sólo para esta sesión)'
$request = [System.Net.FtpWebRequest]::Create($uri)
$request.Method = [System.Net.WebRequestMethods+Ftp]::UploadFile
$request.Credentials = New-Object System.Net.NetworkCredential($ftpUser, $plain)

$contents = [System.IO.File]::ReadAllBytes($LocalFile)
$request.ContentLength = $contents.Length
$rs = $request.GetRequestStream()
$rs.Write($contents, 0, $contents.Length)
$rs.Close()
$response = $request.GetResponse()
Write-Host "Status: $($response.StatusDescription)"; $response.Close()
