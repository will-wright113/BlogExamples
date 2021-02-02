#Connect-AzAccount

Param
(
    [parameter(Mandatory=$true)] [String] $ResourceGroupName,
    [parameter(Mandatory=$true)] [String] $AccountName,
    [parameter(Mandatory=$true)] [String] $FileSystemName,
    [parameter(Mandatory=$false)] [String] $ValidationFile
)

$MaxReturn = 10000
$Total = 0
$Token = $Null

$storageAccount = Get-AzStorageAccount -ResourceGroupName $ResourceGroupName -AccountName $AccountName
$ctx = $storageAccount.Context

$output = $AccountName+"_"+$FileSystemName+".csv"
if (Test-Path $output) 
{
  Remove-Item $output
}

do
{
    $items = Get-AzDataLakeGen2ChildItem -Context $ctx -FileSystem $FileSystemName -Recurse -MaxCount $MaxReturn -ContinuationToken $Token

    $delta = $items | Where-Object {$_.Path.EndsWith("_delta_log")} | Select-Object @{Name="DataType";Expression={"Delta"}},Path,Length,LastModified
    $delta | Export-Csv -Path $output -NoTypeInformation -Append

    $parquet = $items | Where-Object {$_.Path.EndsWith(".parquet") -and ($_.Path -notmatch "snappy.parquet")} | Select-Object @{Name="DataType";Expression={"Parquet"}},Path,Length,LastModified
    $parquet | Export-Csv -Path $output -NoTypeInformation -Append

    $csv = $items | Where-Object {$_.Path.EndsWith(".csv")} | Select-Object @{Name="DataType";Expression={"CSV"}},Path,Length,LastModified
    $csv | Export-Csv -Path $output -NoTypeInformation -Append

    $json = $items | Where-Object {$_.Path.EndsWith(".json") -and ($_.Path -notmatch "_delta_log")} | Select-Object @{Name="DataType";Expression={"JSON"}},Path,Length,LastModified
    $json | Export-Csv -Path $output -NoTypeInformation -Append

    $xml = $items | Where-Object {$_.Path.EndsWith(".xml")} | Select-Object @{Name="DataType";Expression={"XML"}},Path,Length,LastModified
    $xml | Export-Csv -Path $output -NoTypeInformation -Append

    $Total += $items.Count
    Write-Host $Total" items read..."
    if($items.Length -le 0) { Break;}
    $Token = $items[$items.Count -1].ContinuationToken;
}
While ($Token -ne $Null)

Write-Host "Audit Complete!"$Total" items read in total."
Write-Host $delta.Count" Delta datasets found."
Write-Host $parquet.Count" Parquet datasets found."
Write-Host $csv.Count" CSV datasets found."
Write-Host $json.Count" JSON datasets found."
Write-Host $xml.Count" XML datasets found."