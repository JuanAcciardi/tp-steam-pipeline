# Setup inicial del TP en Windows con Docker Desktop. Se corre una sola vez por maquina:
#   powershell -ExecutionPolicy Bypass -File .\scripts\setup.ps1
# Es re-ejecutable: lo que ya existe (.env, dataset, usuario de Airflow) no se pisa.

$ErrorActionPreference = "Stop"
$ProgressPreference = "SilentlyContinue"   # Expand-Archive es muy lento con la barra de progreso
$Repo = Split-Path $PSScriptRoot -Parent
Set-Location $Repo
$RepoSlug = "JuanAcciardi/tp-steam-pipeline"
$DatasetSha256 = "e46e3b55529dbaa533e9c7708b7070bcebb08451a0d626fe228d13bc9f082daa"

Write-Host "1/6 Verificando Docker..."
docker info *> $null
if ($LASTEXITCODE -ne 0) { throw "Docker no esta corriendo: abri Docker Desktop y volve a ejecutar el script." }
# El repo es privado: el dataset se baja con gh (autenticado), no con una URL publica.
gh auth status *> $null
if ($LASTEXITCODE -ne 0) { throw "Falta gh autenticado: instalar GitHub CLI y correr 'gh auth login'." }

Write-Host "2/6 Generando .env (UID + Fernet key para cifrar las Variables)..."
if (-not (Test-Path .env)) {
    $bytes = New-Object byte[] 32
    [Security.Cryptography.RandomNumberGenerator]::Create().GetBytes($bytes)
    $fernet = [Convert]::ToBase64String($bytes).Replace('+', '-').Replace('/', '_')
    "AIRFLOW_UID=50000`nAIRFLOW_FERNET_KEY=$fernet" | Set-Content .env -Encoding ascii
} else { Write-Host "    .env ya existe, se conserva." }
# Los .env creados antes de que existiera AIRFLOW_JWT_SECRET se completan sin tocar el resto.
if (-not (Select-String -Path .env -Pattern '^AIRFLOW_JWT_SECRET=' -Quiet)) {
    $bytes = New-Object byte[] 32
    [Security.Cryptography.RandomNumberGenerator]::Create().GetBytes($bytes)
    $jwt = [Convert]::ToBase64String($bytes).Replace('+', '-').Replace('/', '_')
    "AIRFLOW_JWT_SECRET=$jwt" | Add-Content .env -Encoding ascii
}

Write-Host "3/6 Descargando el dataset de origen (release dataset-v2)..."
New-Item -ItemType Directory -Force data, landing, logs | Out-Null
# Si landing\ tiene otra version del dataset (por ejemplo la v1), se reemplaza por la v2.
$manifest = "landing\_manifest.json"
if ((Test-Path $manifest) -and -not (Select-String -Path $manifest -Pattern '"dataset": "dataset-v2"' -Quiet)) {
    Write-Host "    landing\ tiene otra version del dataset: se reemplaza por dataset-v2."
    Remove-Item landing\* -Recurse -Force
}
if (-not (Test-Path $manifest)) {
    $tmp = Join-Path $env:TEMP "steam-dataset"
    gh release download dataset-v2 -R $RepoSlug -p steam-landing.zip -D $tmp --clobber
    if ($LASTEXITCODE -ne 0) { throw "No se pudo descargar el release dataset-v2" }
    $zip = Join-Path $tmp "steam-landing.zip"
    if ((Get-FileHash $zip -Algorithm SHA256).Hash -ne $DatasetSha256.ToUpper()) { throw "SHA-256 del dataset no coincide" }
    Expand-Archive $zip -DestinationPath landing -Force
    Remove-Item $tmp -Recurse
} else { Write-Host "    landing/ ya tiene el dataset-v2, se conserva." }

Write-Host "4/6 Construyendo la imagen e inicializando Airflow..."
docker compose build
if ($LASTEXITCODE -ne 0) { throw "Fallo docker compose build" }
docker compose up airflow-init
if ($LASTEXITCODE -ne 0) { throw "Fallo airflow-init" }

Write-Host "5/6 Guardando PII_SALT como Airflow Variable (no queda en ningun archivo)..."
$secure = Read-Host "    PII_SALT (el que se paso por el grupo de WhatsApp)" -AsSecureString
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
