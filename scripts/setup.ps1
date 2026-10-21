# Setup inicial del TP en Windows con Docker Desktop. Se corre una sola vez por maquina:
#   powershell -ExecutionPolicy Bypass -File .\scripts\setup.ps1
# Es re-ejecutable: lo que ya existe (.env, dataset, usuario de Airflow) no se pisa.

$ErrorActionPreference = "Stop"
$ProgressPreference = "SilentlyContinue"   # Invoke-WebRequest es muy lento con la barra de progreso
$Repo = Split-Path $PSScriptRoot -Parent
Set-Location $Repo
$DatasetUrl = "https://github.com/JuanAcciardi/tp-steam-pipeline/releases/download/dataset-v1/steam-landing.zip"

Write-Host "1/6 Verificando Docker..."
docker info *> $null
if ($LASTEXITCODE -ne 0) { throw "Docker no esta corriendo: abri Docker Desktop y volve a ejecutar el script." }

Write-Host "2/6 Generando .env (UID + Fernet key para cifrar las Variables)..."
if (-not (Test-Path .env)) {
    $bytes = New-Object byte[] 32
    [Security.Cryptography.RandomNumberGenerator]::Create().GetBytes($bytes)
    $fernet = [Convert]::ToBase64String($bytes).Replace('+', '-').Replace('/', '_')
    "AIRFLOW_UID=50000`nAIRFLOW_FERNET_KEY=$fernet" | Set-Content .env -Encoding ascii
} else { Write-Host "    .env ya existe, se conserva." }

Write-Host "3/6 Descargando el dataset de origen (release dataset-v1)..."
New-Item -ItemType Directory -Force data, landing, logs | Out-Null
if (-not (Test-Path landing\_manifest.json)) {
    $zip = Join-Path $env:TEMP "steam-landing.zip"
    Invoke-WebRequest $DatasetUrl -OutFile $zip
    Expand-Archive $zip -DestinationPath landing -Force
    Remove-Item $zip
} else { Write-Host "    landing/ ya tiene el dataset, se conserva." }

Write-Host "4/6 Construyendo la imagen e inicializando Airflow..."
docker compose build
if ($LASTEXITCODE -ne 0) { throw "Fallo docker compose build" }
docker compose up airflow-init
if ($LASTEXITCODE -ne 0) { throw "Fallo airflow-init" }

Write-Host "5/6 Guardando PII_SALT como Airflow Variable (no queda en ningun archivo)..."
$secure = Read-Host "    PII_SALT (es el mismo para todo el equipo, pediselo a los chicos)" -AsSecureString
$salt = [Runtime.InteropServices.Marshal]::PtrToStringAuto(
    [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secure))
if ([string]::IsNullOrWhiteSpace($salt)) { throw "PII_SALT vacio" }
docker compose run --rm airflow-cli airflow variables set PII_SALT $salt | Out-Null
if ($LASTEXITCODE -ne 0) { throw "No se pudo guardar PII_SALT" }

Write-Host "6/6 Levantando Airflow..."
docker compose up -d
Write-Host ""
Write-Host "Listo: http://localhost:8080  (usuario airflow / clave airflow)"
Write-Host "Activa el DAG steam_medallion_pipeline para correr el backfill 2020-01 -> 2022-12."
