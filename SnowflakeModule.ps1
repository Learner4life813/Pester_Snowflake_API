#External Dependencies
Add-Type -Path "$env:USERPROFILE\.nuget\packages\log4net\2.0.8\lib\net45-full\log4net.dll"
Add-Type -Path "$env:USERPROFILE\.nuget\packages\Newtonsoft.Json\11.0.2\lib\net45\Newtonsoft.Json.dll"
Add-Type -Path "$env:USERPROFILE\.nuget\packages\Snowflake.Data\1.0.4\lib\net46\Snowflake.Data.dll"

. $PSScriptRoot\Utils.ps1

function Connect-Snowflake {
    $EncodedPassword = $Script:EndPointVariables.AmplitudePassword
    $AmplitudeConnect = @{
        host      = $Script:EndPointVariables.AmplitudeHost
        account   = $Script:EndPointVariables.AmplitudeAccount
        user      = $Script:EndPointVariables.AmplitudeUser
        password  = [System.Text.Encoding]::Unicode.GetString([Convert]::FromBase64String($EncodedPassword))
        db        = $Script:EndPointVariables.AmplitudeDatabase
        warehouse = $Script:EndPointVariables.AmplitudeWarehouse
        schema    = $Script:EnvSpecificVariables.AmplitudeSchema
        Table     = $Script:EnvSpecificVariables.AmplitudeTable
    }
    foreach ($kvp in $AmplitudeConnect.GetEnumerator()) {
        $ConnectionString += "$($kvp.Key)=$($kvp.Value);"
    }
    $Script:SnowConnection = New-Object Snowflake.Data.Client.SnowflakeDbConnection
    $Script:SnowConnection.ConnectionString = $ConnectionString
    $Script:SnowConnection.Open()
}

function Disconnect-Snowflake {
    $Script:SnowConnection.Close()
}

function Read-Snowflake {
    [CmdletBinding()]
    [OutputType([System.Data.DataTable])]
    Param
    (
        # The Query to be executed on Snowflake
        [Parameter(Mandatory = $true,
            ValueFromPipelineByPropertyName = $true,
            Position = 0)]
        $Query,
        
        [Parameter(Mandatory = $false,
            ValueFromPipelineByPropertyName = $true,
            Position = 1)]
        [hashtable]$SnowParameter1,

        [Parameter(Mandatory = $false,
            ValueFromPipelineByPropertyName = $true,
            Position = 2)]
        [hashtable]$SnowParameter2,
        
        [Parameter(Mandatory = $false,
            ValueFromPipelineByPropertyName = $true,
            Position = 3)]
        [switch]$OutGridView
    )
    <#Snowflake supports parameterization with a "?" in query.
    However, this doesn't work for TableNames and ColumnNames 
    coz there is no DbType to be specified for these. 
    
    Use ? in queries for parameters in where condition
    Table names and Column names to be injected into the query
    before passing into Read-Snowflake
    #>
    Begin {
        $SnowCmd = New-Object Snowflake.Data.Client.SnowflakeDbCommand($Script:SnowConnection)
        $SnowCmd.CommandText = $Query

        if ($SnowParameter1 -ne $null) {
            $Param1 = $SnowCmd.CreateParameter()
            $Param1.ParameterName = "1"
            $Param1.DbType = $SnowParameter1['DbType']
            $Param1.Value = $SnowParameter1['Value']
            $SnowCmd.Parameters.Add($Param1)
        }

        if ($SnowParameter2 -ne $null) {
            $Param2 = $SnowCmd.CreateParameter()
            $Param2.ParameterName = "2"
            $Param2.DbType = $SnowParameter2['DbType']
            $Param2.Value = $SnowParameter2['Value']
            $SnowCmd.Parameters.Add($Param2)
        }

        $SnowAdapter = New-Object Snowflake.Data.Client.SnowflakeDbDataAdapter
        $SnowAdapter.SelectCommand = $SnowCmd
        $DataSet = New-Object System.Data.DataSet
    }
    Process {
        $SnowAdapter.Fill($DataSet)
    }
    End {
        if ($OutGridView.IsPresent) {
            $DataSet.Tables[0] | Out-GridView
        }
        else {
            return $DataSet.Tables[0]
        }
    }
}

function Get-SnowRecordCount {
    param(
        [Parameter(Mandatory = $true,
            ValueFromPipelineByPropertyName = $true,
            Position = 0)]
        [string]$ServerUploadTimeFrom,
        [Parameter(Mandatory = $true,
            ValueFromPipelineByPropertyName = $true,
            Position = 1)]
        [string]$ServerUploadTimeTo
    )
    Begin {
        
        $SnowQuery = "select count(*) 
                from $($Script:EnvSpecificVariables.AmplitudeTable)
                where (event_type ILIKE 'Streams -%Card - %' 
                    OR event_type ILIKE 'CONTENT%' 
                    OR event_type ILIKE 'Resources%') 
                AND length(user_id) = 36
                AND SERVER_UPLOAD_TIME >= ?
                AND SERVER_UPLOAD_TIME < ?"
        $Param1 = @{
            DbType = 'string'
            Value  = $ServerUploadTimeFrom
        }
        $Param2 = @{
            DbType = 'string'
            Value  = $ServerUploadTimeTo
        }
    }
    Process {
        $DataTable = Read-Snowflake $SnowQuery $Param1 $Param2
    }
    End {
        return $DataTable.item(3)[0]
    }
}

function Get-SnowColumnNames {
    Begin {
        $SnowQuery = "select Column_name from Information_schema.columns
                    where table_name = ?
                    order by ordinal_position"
        $Param1 = @{
            DbType = 'string'
            Value  = $($Script:EnvSpecificVariables.AmplitudeTable)
        }
        $ColumnNames = New-Object System.Collections.Generic.List[System.Object] 
    }
    Process {
        $DataTable = Read-Snowflake $SnowQuery $Param1
        for($i = 2; $i -lt $DataTable.count; $i++)
        {
            $ColumnNames.Add($DataTable.item($i)[0])
        }
        $SnowColumnNames = $ColumnNames | Where-Object {$_ -ne $null}
    }
    End {
        Return $SnowColumnNames
    }
}

function Compare-SnowSchema {
    Begin {
        $ExistingColumnNames = $Script:EndPointVariables.AmplitudeColumns
        $SnowColumnNames = Get-SnowColumnNames
    }
    Process {
        #any null values in the array needs to be removed. Otherwise, Compare-Object throws a termination error
        $ColNamesMatch = @(Compare-Object $ExistingColumnNames $SnowColumnNames).Count -eq 0
    }
    End {
        Return $ColNamesMatch
    }
}

function Find-SnowDataMatchingRegex {
    param(
        [Parameter(Mandatory = $true,
            ValueFromPipelineByPropertyName = $true,
            Position = 0)]
        [string]$RegexPattern
    )
    Begin {
        $SnowColumnNames = Get-SnowColumnNames "$Script:EnvSpecificVariables.AmplitudeTable"
        #AMPLITUDE_ATTRIBUTION_IDS must be removed from the list because regex on it gives error
        $SnowColumnNames = $SnowColumnNames | ? {$_ -ne 'AMPLITUDE_ATTRIBUTION_IDS'}
        $RegexQuery = "select count(*) 
                from $($Script:EnvSpecificVariables.AmplitudeTable)
                where ColName regexp ?
                "
        $Param1 = @{
            DbType = 'string'
            Value  = $RegexPattern
        } 
    }
    Process {
        $CountOfFieldsMatchingRegex = 0
        foreach ($ColName in $SnowColumnNames) {
            $CountOfFieldsMatchingRegex += (Read-Snowflake $RegexQuery.Replace("ColName", $ColName) $Param1).item(2)[0]
        }
    }
    End {
        Return $CountOfFieldsMatchingRegex
    }
}
