# Copyright (c) 2026 Databricks, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Invoke-Checked {
    param(
        [Parameter(Mandatory = $true)][string]$FilePath,
        [Parameter(Mandatory = $false)][AllowEmptyCollection()][string[]]$Arguments = @()
    )

    & $FilePath @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "command failed with exit code ${LASTEXITCODE}: $FilePath $($Arguments -join ' ')"
    }
}

function Find-AsciiMatch {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][regex]$Pattern
    )

    $buffer = [byte[]]::new(65536)
    $tail = ""
    $stream = [System.IO.File]::OpenRead($Path)
    try {
        while (($bytesRead = $stream.Read($buffer, 0, $buffer.Length)) -gt 0) {
            $chunk = $tail + [System.Text.Encoding]::ASCII.GetString($buffer, 0, $bytesRead)
            $match = $Pattern.Match($chunk)
            if ($match.Success) {
                return $match.Value
            }
            $tailLength = [Math]::Min(1024, $chunk.Length)
            $tail = $chunk.Substring($chunk.Length - $tailLength)
        }
    } finally {
        $stream.Dispose()
    }
    return $null
}

$profile = "release-with-debuginfo"
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "../..")).Path
$targetRoot = if ($env:CARGO_TARGET_DIR) {
    [System.IO.Path]::GetFullPath($env:CARGO_TARGET_DIR, $repoRoot)
} else {
    Join-Path $repoRoot "target"
}
$profileDir = Join-Path $targetRoot $profile
$source = Join-Path $repoRoot "tests/symbols/staticlib_consumer.c"
$includeDir = Join-Path $repoRoot "include"
$archive = Join-Path $profileDir "databricks_sql_kernel.lib"
$object = Join-Path $profileDir "staticlib_consumer.obj"
$executable = Join-Path $profileDir "kernel-symbol-smoke.exe"
$pdb = Join-Path $profileDir "kernel-symbol-smoke.pdb"

New-Item -ItemType Directory -Force -Path $profileDir | Out-Null
Push-Location $repoRoot
try {
    # Build the exact static archive under test and capture the MSVC system
    # libraries that rustc says its final native linker must receive.
    $buildLines = @(& cargo rustc --locked --profile $profile --lib -- --print=native-static-libs 2>&1)
    $buildExit = $LASTEXITCODE
    $buildLines | ForEach-Object { Write-Host $_ }
    if ($buildExit -ne 0) {
        throw "cargo rustc failed with exit code $buildExit"
    }
    if (-not (Test-Path -LiteralPath $archive -PathType Leaf)) {
        throw "Cargo did not produce $archive"
    }

    $nativeLine = $buildLines |
        ForEach-Object { "$_" } |
        Where-Object { $_ -match "native-static-libs:\s*(.+)$" } |
        Select-Object -Last 1
    if (-not $nativeLine) {
        throw "rustc did not report native-static-libs"
    }
    $nativeText = ([regex]::Match($nativeLine, "native-static-libs:\s*(.+)$")).Groups[1].Value.Trim()
    $nativeLinkArgs = @($nativeText -split "\s+" | Where-Object { $_ })

    Invoke-Checked "cl.exe" @(
        "/nologo", "/c", "/O2", "/MD", "/Z7", "/I$includeDir", $source, "/Fo$object"
    )
    $linkArgs = @(
        "/NOLOGO", "/DEBUG:FULL", "/OPT:REF", "/OPT:ICF", "/INCREMENTAL:NO",
        "/PDB:$pdb", "/PDBALTPATH:%_PDB%", "/OUT:$executable",
        $object, $archive
    ) + $nativeLinkArgs
    Invoke-Checked "link.exe" $linkArgs
    if (-not (Test-Path -LiteralPath $pdb -PathType Leaf)) {
        throw "link.exe did not produce $pdb"
    }
    Invoke-Checked $executable @()

    if (-not (Get-Command "dumpbin.exe" -ErrorAction SilentlyContinue)) {
        throw "dumpbin.exe is required to validate the executable/PDB CodeView identity"
    }

    $functionPattern = [regex]::new("kernel_get_last_error")
    $sourcePattern = [regex]::new("(?i)src[\\/]c_abi[\\/]error\.rs")
    if (-not (Find-AsciiMatch $pdb $functionPattern)) {
        throw "PDB does not describe kernel_get_last_error"
    }
    if (-not (Find-AsciiMatch $pdb $sourcePattern)) {
        throw "PDB does not name src/c_abi/error.rs"
    }

    # The PE carries only the CodeView locator/identity; source and function
    # records stay in the separately published PDB. In an isolated directory,
    # dumpbin reports the adjacent PDB only when its GUID and age match the PE.
    $checkDir = Join-Path ([System.IO.Path]::GetTempPath()) ("kernel-pdb-check-" + [guid]::NewGuid().ToString("N"))
    New-Item -ItemType Directory -Path $checkDir | Out-Null
    try {
        $checkExe = Join-Path $checkDir (Split-Path -Leaf $executable)
        $checkPdb = Join-Path $checkDir (Split-Path -Leaf $pdb)
        Copy-Item -LiteralPath $executable -Destination $checkExe
        Copy-Item -LiteralPath $pdb -Destination $checkPdb

        $lookup = @(& dumpbin.exe /NOLOGO /PDBPATH $checkExe 2>&1)
        $lookupExit = $LASTEXITCODE
        $lookupText = $lookup | Out-String
        Write-Host $lookupText
        if ($lookupExit -ne 0) {
            throw "dumpbin /PDBPATH failed with exit code $lookupExit"
        }
        if ($lookupText -notmatch [regex]::Escape($checkPdb)) {
            throw "dumpbin did not resolve the supplied PDB; its GUID/age may not match the executable"
        }
    } finally {
        Remove-Item -LiteralPath $checkDir -Recurse -Force -ErrorAction SilentlyContinue
    }

    Write-Host "ok: Windows PDB contains kernel Rust source/function info"
    Write-Host "ok: dumpbin validated the executable/PDB CodeView GUID and age"
} finally {
    Pop-Location
}
