[CmdletBinding()]
param([string]$RepositoryRoot = (Join-Path $PSScriptRoot '..\..'))

$ErrorActionPreference = 'Stop'
function Fail([string]$Message) { throw "[staged-credential-boundary] $Message" }

$repository = [IO.Path]::GetFullPath($RepositoryRoot)
$paths = @(git -C $repository diff --cached --name-only --diff-filter=ACMR)
if ($LASTEXITCODE -ne 0) { Fail 'unable to inspect the Git index.' }

$knownSecret = '(?i)(-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----|github_pat_[A-Za-z0-9_]{20,}|gh[pousr]_[A-Za-z0-9]{30,}|AKIA[0-9A-Z]{16}|xox[baprs]-[A-Za-z0-9-]{20,}|sk-[A-Za-z0-9]{32,})'
foreach ($path in $paths) {
  $normalized = $path.Replace('\', '/')
  $isTemplate = $normalized -match '(^|/)(?:secrets|env)/(?:README\.md|\.gitkeep)$' -or $normalized -match '(^|/)\.env\.example$'
  if (-not $isTemplate -and ($normalized -match '(^|/)(?:secrets|env|local)/' -or $normalized -match '(^|/)\.env(?:\.|$)' -or $normalized -match '(?i)\.(?:key|pem|p12|pfx|jks)$')) {
    Fail "credential-bearing path cannot be committed: $normalized"
  }
  $content = git -C $repository show ":$path" 2>$null
  if ($LASTEXITCODE -eq 0 -and (($content -join "`n") -match $knownSecret)) {
    Fail "credential-like content cannot be committed: $normalized"
  }
}

Write-Output 'STAGED_CREDENTIAL_BOUNDARY=PASS'
