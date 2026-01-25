#!/usr/bin/env pwsh
# NLdoc Conversion Test Suite
# Tests the conversion pipeline via API without editor complexity

param(
    [string]$TestFile = "",
    [string]$ApiBase = "https://api.nldoc.commonground.nu",
    [switch]$SkipUpload
)

$ErrorActionPreference = "Stop"

function Write-TestHeader($msg) {
    Write-Host "`n========================================" -ForegroundColor Cyan
    Write-Host " $msg" -ForegroundColor Cyan
    Write-Host "========================================" -ForegroundColor Cyan
}

function Write-TestResult($name, $success, $details = "") {
    if ($success) {
        Write-Host "[PASS] $name" -ForegroundColor Green
    } else {
        Write-Host "[FAIL] $name" -ForegroundColor Red
    }
    if ($details) {
        Write-Host "       $details" -ForegroundColor Gray
    }
}

# Test 1: API Health Check
Write-TestHeader "Test 1: API Health Check"
try {
    $health = Invoke-WebRequest -Uri "$ApiBase/health" -Method GET -TimeoutSec 10 -ErrorAction SilentlyContinue
    Write-TestResult "API responds" $true "Status: $($health.StatusCode)"
} catch {
    Write-TestResult "API responds" $false $_.Exception.Message
    # Try to continue anyway
}

# Test 2: CORS Preflight
Write-TestHeader "Test 2: CORS Preflight"
try {
    $cors = Invoke-WebRequest -Uri "$ApiBase/conversion" -Method OPTIONS -Headers @{
        "Origin" = "https://editor.nldoc.commonground.nu"
        "Access-Control-Request-Method" = "POST"
        "Access-Control-Request-Headers" = "content-type"
    } -TimeoutSec 10 -ErrorAction SilentlyContinue
    $allowOrigin = $cors.Headers["Access-Control-Allow-Origin"]
    Write-TestResult "CORS preflight" ($allowOrigin -ne $null) "Allow-Origin: $allowOrigin"
} catch {
    Write-TestResult "CORS preflight" $false $_.Exception.Message
}

# Test 3: File Upload (if file provided)
if ($TestFile -and (Test-Path $TestFile) -and -not $SkipUpload) {
    Write-TestHeader "Test 3: File Upload"
    
    $fileName = Split-Path $TestFile -Leaf
    $fileExt = [System.IO.Path]::GetExtension($fileName).ToLower()
    $fileSize = (Get-Item $TestFile).Length
    
    Write-Host "File: $fileName ($([math]::Round($fileSize/1024, 2)) KB)" -ForegroundColor Gray
    
    # Determine content type
    $contentType = switch ($fileExt) {
        ".pdf" { "application/pdf" }
        ".docx" { "application/vnd.openxmlformats-officedocument.wordprocessingml.document" }
        default { "application/octet-stream" }
    }
    
    try {
        # Create multipart form data
        $boundary = [System.Guid]::NewGuid().ToString()
        $fileBytes = [System.IO.File]::ReadAllBytes($TestFile)
        $fileBase64 = [Convert]::ToBase64String($fileBytes)
        
        # Use curl for multipart (more reliable)
        $curlCmd = "curl -s -w '`n%{http_code}' -X POST '$ApiBase/conversion' " +
                   "-H 'Origin: https://editor.nldoc.commonground.nu' " +
                   "-H 'X-Target-Content-Type: text/html' " +
                   "-F 'file=@$TestFile;type=$contentType' " +
                   "--max-time 30"
        
        Write-Host "Uploading..." -ForegroundColor Gray
        $result = Invoke-Expression $curlCmd 2>&1
        $lines = $result -split "`n"
        $httpCode = $lines[-1]
        $body = ($lines[0..($lines.Length-2)]) -join "`n"
        
        if ($httpCode -eq "200") {
            Write-TestResult "File upload" $true "HTTP $httpCode"
            
            # Parse response for document ID
            try {
                $json = $body | ConvertFrom-Json
                # API returns: { data: [ { uuid: ... } ] }
                $docId = $null
                if ($json.data -and $json.data.Count -gt 0 -and $json.data[0].uuid) {
                    $docId = $json.data[0].uuid
                } elseif ($json.uuid) {
                    # Backwards compatibility
                    $docId = $json.uuid
                }

                if ($docId) {
                    Write-Host "       Document ID: $docId" -ForegroundColor Gray
                    $script:DocumentId = $docId
                } else {
                    Write-Host "       Could not parse Document ID from response" -ForegroundColor Yellow
                }
            } catch {
                Write-Host "       Response: $($body.Substring(0, [Math]::Min(200, $body.Length)))..." -ForegroundColor Gray
            }
        } else {
            Write-TestResult "File upload" $false "HTTP $httpCode - $body"
        }
    } catch {
        Write-TestResult "File upload" $false $_.Exception.Message
    }
}

# Test 4: SSE Stream (if we have a document ID)
if ($script:DocumentId) {
    Write-TestHeader "Test 4: SSE Event Stream"
    
    try {
        Write-Host "Connecting to SSE stream for $($script:DocumentId)..." -ForegroundColor Gray
        Write-Host "Waiting up to 120 seconds for events..." -ForegroundColor Gray
        
        # Use curl for SSE
        $sseCmd = "curl -s -N '$ApiBase/conversion/$($script:DocumentId)' " +
                  "-H 'Accept: text/event-stream' " +
                  "-H 'Origin: https://editor.nldoc.commonground.nu' " +
                  "--max-time 120"
        
        # Run curl in foreground (more reliable than background job for streaming output)
        $output = Invoke-Expression $sseCmd 2>&1
        $raw = if ($output -is [string]) { $output } else { ($output -join "`n") }

        $completed = $raw.Contains("https://event.spec.nldoc.nl/done")
        if ($completed) {
            if ($raw -match '\"location\"\\s*:\\s*\"([^\"]+)\"') {
                $script:OutputLocation = $Matches[1]
                Write-Host "  Output: $($script:OutputLocation)" -ForegroundColor Green
            } else {
                # Fallback: for our HTML flow location is always "{docId}.html"
                $script:OutputLocation = "$($script:DocumentId).html"
                Write-Host "  Done event received; using fallback location: $($script:OutputLocation)" -ForegroundColor Yellow
            }
        }

        if ($raw.Contains("https://event.spec.nldoc.nl/error")) {
            Write-Host "  Error event received" -ForegroundColor Red
        }
        
        Write-TestResult "SSE stream" $completed "Completed: $completed"
        
    } catch {
        Write-TestResult "SSE stream" $false $_.Exception.Message
    }
}

# Test 5: Download Output (if we have location)
if ($script:OutputLocation) {
    Write-TestHeader "Test 5: Download Output"
    
    try {
        # API client downloads via GET /file/{location}
        $downloadUrl = "$ApiBase/file/$($script:OutputLocation)"
        Write-Host "Downloading: $downloadUrl" -ForegroundColor Gray
        
        $outFile = "test-output-$(Get-Date -Format 'yyyyMMdd-HHmmss').html"
        Invoke-WebRequest -Uri $downloadUrl -OutFile $outFile -TimeoutSec 30
        
        $fileSize = (Get-Item $outFile).Length
        $content = Get-Content $outFile -Raw
        $hasHtml = $content -match "<html"
        $hasBody = $content -match "<body"
        $hasTable = $content -match "<table"
        $hasList = ($content -match "<ul") -or ($content -match "<ol")
        
        Write-TestResult "Download output" $true "Size: $([math]::Round($fileSize/1024, 2)) KB"
        Write-TestResult "Valid HTML structure" ($hasHtml -and $hasBody)
        Write-TestResult "Contains tables or lists" ($hasTable -or $hasList)
        
        Write-Host "`nOutput saved to: $outFile" -ForegroundColor Green
        Write-Host "First 500 chars:" -ForegroundColor Gray
        Write-Host $content.Substring(0, [Math]::Min(500, $content.Length)) -ForegroundColor Gray
        
    } catch {
        Write-TestResult "Download output" $false $_.Exception.Message
    }
}

# Summary
Write-TestHeader "Test Summary"
Write-Host "Tests completed at $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')" -ForegroundColor Gray

if ($script:OutputLocation) {
    Write-Host "`n[SUCCESS] Pipeline completed!" -ForegroundColor Green
    Write-Host "Output: $ApiBase/file/$($script:OutputLocation)" -ForegroundColor Green
} else {
    Write-Host "`n[INCOMPLETE] Pipeline did not complete" -ForegroundColor Yellow
    Write-Host "Check logs with:" -ForegroundColor Gray
    Write-Host "  kubectl -n nldoc logs -l app.kubernetes.io/component=worker.document-source --tail=20" -ForegroundColor Gray
    Write-Host "  kubectl -n nldoc logs -l app.kubernetes.io/component=worker.folio-spec-python --tail=50" -ForegroundColor Gray
}


