# Airflow Plugins

This is some operators and plugins that me (brenno.flavio@fluxoresultados.com.br) did over time working with Airflow and data engineering. Feel free to use! If this helps you, feel free to contribute opening PR's with new operators and changes in code.

## How to use

Just copy this into your Airflow's Plugins folder. You import the plugin as an Operator, following the pattern: `from airflow.operators.<file_name> import <OperatorName>`. Example: `from airflow.operators.postgres_utils import PostgresToS3Operator`

## How to contribute

Just open a PR with your stuff, let's make this operators better and in the future upstream it!

## Avaliable Plugins:
- date
    - start
    - end
    - truncate
    - fmtnow
    - format
    - format_nodash

- dynamo
    - S3ToDynamoDBOperator
    - XcomDynamoDBKeyOperator

- file
    - ExcelToCSVOperator
    - StandardizeCSVHeaderOperator
    - ConcatenateCSVOperator
    - BulkExcelToCSVOperator
    - BulkCopyS3Operator
    - CSVToJsonOperator

- firestore
    - S3ToFirestoreOperator
    - FirestoreHook

- ftp
    - BulkFTPToS3Operator
    - FTPSNoHostHook

- mssql
    - MsSQLToS3Operator
    - MsSqlOperator
    - S3ToMsSqlOverBcpOperator
    - CreatePrefixTableMsSqlOperator
    - DropPrefixTableMsSqlOperator
    - MsSqlHook
    - BcpHook

- multiprocessing
    - PythonThreadingOperator

- postgres
    - S3ToPostgresOperator
    - CreatePrefixTablePostgresOperator
    - DropPrefixTablePostgresOperator
    - PostgresXcomOperator
    - PostgresToS3Operator

- requests
    - RequestToS3Operator

- sftp
    - BulkS3ToSFTPOperator
    - BulkSFTPToS3Operator
    - RenameSFTPOperator
    - BulkRenameSFTPOperator
    - DeleteFileSFTPOperator
    - BulkDeleteFileSFTPOperator
    - SftpMultipleAuthenticationHook
