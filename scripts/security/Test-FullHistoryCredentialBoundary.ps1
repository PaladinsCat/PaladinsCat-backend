[CmdletBinding()]
param([string]$RepositoryRoot = (Join-Path $PSScriptRoot '..\..'))

$ErrorActionPreference = 'Stop'
function Fail([string]$Message) { throw "[history-credential-boundary] $Message" }

$repository = [IO.Path]::GetFullPath($RepositoryRoot)
$paths = @(git -C $repository log --all --name-only --format=)
if ($LASTEXITCODE -ne 0) { Fail 'unable to enumerate historical paths.' }
foreach ($path in $paths) {
  $normalized = $path.Replace('\', '/')
  $isTemplate = $normalized -match '(^|/)(?:secrets|env)/(?:README\.md|\.gitkeep)$' -or $normalized -match '(^|/)\.env\.example$'
  if (-not $isTemplate -and ($normalized -match '(^|/)(?:secrets|env|local)/' -or $normalized -match '(^|/)\.env(?:\.|$)' -or $normalized -match '(?i)\.(?:key|pem|p12|pfx|jks)$')) {
    Fail "credential-bearing path exists in history: $normalized"
  }
}

$knownSecret = '(?i)(-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----|github_pat_[A-Za-z0-9_]{20,}|gh[pousr]_[A-Za-z0-9]{30,}|AKIA[0-9A-Z]{16}|xox[baprs]-[A-Za-z0-9-]{20,}|sk-[A-Za-z0-9]{32,})'
$seen = @{}
foreach ($entry in @(git -C $repository rev-list --objects --all)) {
  if ($entry -notmatch '^(?<oid>[0-9a-f]{40})(?:\s+(?<path>.+))?$') { continue }
  $oid = $Matches.oid
  $path = $Matches.path
  if ($seen[$oid] -or $path -notmatch '(?i)\.(?:cfg|conf|env|ini|json|md|mjs|ps1|py|rs|sh|toml|ts|tsx|txt|yaml|yml)$') { continue }
  $seen[$oid] = $true
  [int64]$size = git -C $repository cat-file -s $oid
  if ($LASTEXITCODE -ne 0 -or $size -gt 2MB) { continue }
  $content = git -C $repository cat-file blob $oid
  if ($LASTEXITCODE -eq 0 -and (($content -join "`n") -match $knownSecret)) {
    Fail "credential-like content exists in history: $path"
  }
}

Write-Output 'FULL_HISTORY_CREDENTIAL_BOUNDARY=PASS'
