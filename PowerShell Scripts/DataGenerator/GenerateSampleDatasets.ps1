Param 
(
	[parameter(Mandatory=$true)] [String] $config_file
)

Write-Host "Installing NameIT Module..."
Install-Module NameIT -Scope CurrentUser

$StartTime = $(Get-Date)
$config_path = "Config/"+$config_file

Write-Host "Reading Config..."
$config = Get-Content -Raw -Path $config_path | ConvertFrom-Json

Write-Host "Creating Custom Data Types..."
$custom_data = @{}
foreach ($data_type in $config.custom_data_types) {
    Write-Host "Name:"$data_type.name", Value(s):"$data_type.value
    $custom_data.Add($data_type.name, $data_type.value)
}

Write-Host "Creating Keys..."
foreach ($key in $config.keys) {

    $value = Invoke-Generate $key.key_value -Count $key.row_count -SetSeed $key.seed
    Write-Host "Name:"$key.key_name", Count:"$key.row_count
    $custom_data.Add($key.key_name, $value)
}

Write-Host "Creating Datasets..."
foreach ($dataset in $config.datasets) {
    # Create Schema
    foreach ($column in $dataset.schema) {
        $schema = $schema + $column.column_name + " = " + $column.column_value + "`n"
    }

    # Generate Data
    Write-Host "Name:"$dataset.filename", Columns:"$dataset.schema.Count", Row Count:"$dataset.row_count", Append:"$derived_dataset.append
    $data = Invoke-Generate $schema -CustomData $custom_data -AsPSObject -Count $dataset.row_count -SetSeed $dataset.seed 
    $output_path = "Output/"+$dataset.filename
    if ($dataset.append -eq "True") {
        $data | Select-Object $dataset.order | Export-Csv -Path $output_path -NoTypeInformation -Append
    } else {
        $data | Select-Object $dataset.order | Export-Csv -Path $output_path -NoTypeInformation 
    }
}

Write-Host "Finished!"
$elapsedTime = $(Get-Date) - $StartTime
$totalTime = "{0:HH:mm:ss}" -f ([datetime]$elapsedTime.Ticks)
$time_output = "Time Taken: "+$totalTime
Write-Host $time_output