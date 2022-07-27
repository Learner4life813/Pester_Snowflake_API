param(
    [Parameter(Mandatory = $true,
        ValueFromPipelineByPropertyName = $true,
        Position = 0)]
    [Validateset('Local', 'Dev', 'Test')]
    $EnvUnderTest
)

#External Dependencies
. $PSScriptRoot\Utils.ps1
. $PSScriptRoot\SnowflakeModule.ps1

#Global BeforeAll
Get-Config $EnvUnderTest #initializes variables from config in all modules
Connect-Snowflake

Test-ECV $EnvUnderTest

Describe 'Compare the record counts on Snowflake and SQL' {
    BeforeAll {
        #Get details of the most recent poll
        $LatestPollRow = Get-LatestPoll
        $StartTimeStamp = $LatestPollRow.StartTimeStamp.ToString("yyyy-MM-dd HH:mm:ss")
        $EndTimeStamp = $LatestPollRow.EndTimeStamp.ToString("yyyy-MM-dd HH:mm:ss")
        $Status = $LatestPollRow.Status
        $SQLRecordCount = $LatestPollRow.RecordCount
        
        #Get the record count with the SQL time stamps
        $SnowRecordCount = Get-SnowRecordCount -ServerUploadTimeFrom $StartTimeStamp -ServerUploadTimeTo $EndTimeStamp
    }
    It 'Should have the latest poll a success' {
        $Status | Should Be 'Success'
    }
    It 'Should have the record counts equal' {
        $SnowRecordCount | Should Be $SQLRecordCount
    }
    It 'Should have the Snowflake record count > 0' {
        $SnowRecordCount | Should -BeGreaterThan 0
    }
    It 'Should have the output file row count > 0' {
        $SQLRecordCount | Should -BeGreaterThan 0
    }
}

Describe 'Last poll is in the last 24 hrs' {
    BeforeAll {
        $UTCNow = (Get-Date).ToUniversalTime()
        $ADayAfterLastPoll = ((Get-LastPollDate).AddHours(24)).DateTime
    }
    It 'Last poll is in the last 24 hrs' {
        $ADayAfterLastPoll | Should -BeGreaterThan $UTCNow
    }
}

Describe 'Check Snowflake data' {
    It 'Should have the schema unchanged' {
        Compare-SnowSchema | Should be $true 
    }
    It 'Should have no embedded tabs' {
        Find-SnowDataMatchingRegex '.*[\t].*' | Should Be 0
    }
}

#Global AfterAll
Disconnect-Snowflake
