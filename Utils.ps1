#External dependencies
. $PSScriptRoot\Config.ps1
Import-Module (Join-Path -Path (Split-Path (Split-Path $PSScriptRoot -Parent) -Parent) -ChildPath 'Arc\Automation.psm1')

function Get-Config {
    param(
        [Parameter(Mandatory = $true,
            ValueFromPipelineByPropertyName = $true,
            Position = 0)]
        [string]$EnvUnderTest)
    $Script:EndPointVariables = $Config.EndpointValues
    $Script:EnvSpecificVariables = iex "`$Config.$EnvUnderTest"
}

function Get-ECV {
    $ECVUri = [System.UriBuilder]::new($Script:EnvSpecificVariables.Protocol,
        $Script:EnvSpecificVariables.Domain,
        $Script:EndPointVariables.PortNum,
        $Script:EndPointVariables.ECV)
    $ECV = Invoke-WebRequest $ECVUri.Uri.ToString() -Method 'GET' -ContentType $($Script:EndPointVariables.ContentType)
    return $ECV
}

function Read-SQL {
    [CmdletBinding()]
    Param
    (
        # The Query to be executed on SQL Server
        [Parameter(Mandatory = $true,
            ValueFromPipelineByPropertyName = $true,
            Position = 0)]
        $Query
    )
    try {
        Invoke-SqlServerQuery -SqlServerName $Script:EnvSpecificVariables.Server -Database $Script:EnvSpecificVariables.DBInstance -Query $Query -ErrorAction 'Stop'
    }
    catch {
        Write-Error $_
    }
}

function Test-ECV {
    Describe 'Should pass ECV checks' -Tag "BVT" {
        BeforeAll {
            $ECV = Get-ECV
            $ECVContent = $ECV.Content | ConvertFrom-Json
            $DatabaseCheck = $ECVContent.resources | where {$_.name -eq "Database"}
        }
        It 'Should get response 200' {
            $ECV.StatusCode | Should Be 200
            $ECV.StatusDescription | Should Be "OK"
        }
        It 'Should return metrics' {
            $ECVContent.metrics | % {$_ | Should Not Be $null}
        }
        It 'Should return Database exists' {
            $DatabaseCheck.passed | Should be $true
        }
    }
}
