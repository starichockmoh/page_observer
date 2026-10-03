param([switch]$Race)
$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot
$previousCC = $env:CC
$previousCGO = $env:CGO_ENABLED
Push-Location $projectRoot
try {
    $goCommand = Get-Command go -ErrorAction SilentlyContinue
    $goBinary = if ($goCommand) { $goCommand.Source } else { 'C:\Program Files\Go\bin\go.exe' }
    if (-not (Test-Path -LiteralPath $goBinary)) { throw 'Go is not installed or is not in PATH.' }
    if ($Race) {
        $compiler = Get-ChildItem -LiteralPath (Join-Path $projectRoot '.tools') -Filter 'llvm-mingw-*-ucrt-x86_64' -Directory -ErrorAction SilentlyContinue | Sort-Object Name -Descending | Select-Object -First 1
        if ($compiler) {
            $env:CC = (Join-Path $compiler.FullName 'bin\clang.exe').Replace('\','/')
        }
        $env:CGO_ENABLED = '1'
        & $goBinary test -race ./...
    } else {
        & $goBinary test ./...
    }
    if ($LASTEXITCODE -ne 0) { throw 'Go tests failed.' }
    & $goBinary vet ./...
    if ($LASTEXITCODE -ne 0) { throw 'Go vet failed.' }
} finally {
    $env:CC = $previousCC
    $env:CGO_ENABLED = $previousCGO
    Pop-Location
}
