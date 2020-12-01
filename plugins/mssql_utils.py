import csv
from contextlib import closing
from tempfile import NamedTemporaryFile
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
import pyodbc
from airflow.hooks.dbapi_hook import DbApiHook
import subprocess
from airflow import macros
import botocore


class MsSqlHook(DbApiHook):
    """
    Interact with Microsoft SQL Server.
    """

    conn_name_attr = "mssql_conn_id"
    default_conn_name = "mssql_default"
    supports_autocommit = True

    def __init__(self, *args, **kwargs):
        super(MsSqlHook, self).__init__(*args, **kwargs)
        self.schema = kwargs.pop("schema", None)
        if "autocommit" in args:
            self.autocommit = args["autocommit"]
        else:
            self.autocommit = True

    def get_conn(self):
        """
        Returns a mssql connection object
        """
        conn = self.get_connection(self.mssql_conn_id)
        host_port = conn.host + "," + str(conn.port)
        conn = pyodbc.connect(
            server=host_port,
            uid=conn.login,
            pwd=conn.password,
            database=self.schema or conn.schema,
            driver=conn.extra,
            autocommit=self.autocommit,
        )
        return conn

    def set_autocommit(self, conn, autocommit):
        conn.autocommit = autocommit

    def get_autocommit(self, conn):
        return conn.autocommit


class MsSQLToS3Operator(BaseOperator):
    template_fields = ("sql", "dest_key")
    template_ext = (".sql",)

    @apply_defaults
    def __init__(
        self,
        sql,
        dest_key,
        dest_bucket,
        sql_parameters=None,
        database=None,
        mssql_conn_id="mssql_default",
        aws_conn_id="aws_default",
        persist_header=True,
        csv_delimiter=",",
        csv_replace="",
        upload_if_no_rows=False,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.mssql_conn_id = mssql_conn_id
        self.aws_conn_id = aws_conn_id
        self.sql = sql
        self.database = database
        self.dest_key = dest_key
        self.dest_bucket = dest_bucket
        self.sql_parameters = sql_parameters
        self.persist_header = persist_header
        self.csv_delimiter = csv_delimiter
        self.csv_replace = csv_replace
        self.upload_if_no_rows = upload_if_no_rows

    def sanitize(self, word):
        try:
            return word.replace("\x00", "").replace(
                self.csv_delimiter, self.csv_replace
            )
        except Exception:
            return word

    def execute(self, context):
        self.log.info("Start executing MsSQLToS3Operator")
        mssql_hook = MsSqlHook(mssql_conn_id=self.mssql_conn_id, schema=self.database)
        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)

        with closing(mssql_hook.get_conn()) as connection:
            with connection, connection.cursor() as cursor:
                self.log.info(f"Going to execute query {self.sql}")
                if self.sql_parameters:
                    cursor.execute(self.sql, self.sql_parameters)
                else:
                    cursor.execute(self.sql)
                columns = [description[0].lower() for description in cursor.description]

                self.log.info(f"Got columns {columns}")
                with NamedTemporaryFile("w", encoding="utf-8") as file:
                    csv_file = csv.writer(
                        file, lineterminator="\n", delimiter=self.csv_delimiter
                    )

                    if self.persist_header:
                        csv_file.writerow(columns)

                    i = 0
                    for row in cursor:
                        i += 1
                        csv_file.writerow(self.sanitize(value) for value in row)
                    file.flush()

                    self.log.info("Finished writing temp file")

                    if i < 1 and not self.upload_if_no_rows:
                        self.log.info("CSV Returned no Rows. Skipping")
                        return False

                    s3_hook.load_file(
                        filename=file.name,
                        key=self.dest_key,
                        bucket_name=self.dest_bucket,
                        replace=True,
                    )
                    self.log.info("Finished executing MsSQLToS3Operator")

                    return True


class MsSqlOperator(BaseOperator):
    template_fields = ("sql",)
    template_ext = (".sql",)
    ui_color = "#ededed"

    @apply_defaults
    def __init__(
        self,
        sql,
        mssql_conn_id="mssql_default",
        parameters=None,
        autocommit=False,
        database=None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.mssql_conn_id = mssql_conn_id
        self.sql = sql
        self.parameters = parameters
        self.autocommit = autocommit
        self.database = database

    def execute(self, context):
        if not self.sql.replace("\n", "").replace(" ", "").lower().startswith("with"):
            if "SET NOCOUNT ON" not in self.sql:
                self.sql = f"SET NOCOUNT ON\n{self.sql}"
        self.log.info("Executing: %s", self.sql)
        hook = MsSqlHook(mssql_conn_id=self.mssql_conn_id, schema=self.database)
        hook.run(self.sql, autocommit=self.autocommit, parameters=self.parameters)


class S3ToMsSqlOverBcpOperator(BaseOperator):
    template_fields = ("source_key", "dest_table")

    @apply_defaults
    def __init__(
        self,
        source_key,
        source_bucket,
        dest_table,
        mssql_database,
        aws_conn_id="aws_default",
        mssql_conn_id="mssql_default",
        delimiter="\t",
        replace=" ",
        truncate=False,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.source_key = source_key
        self.source_bucket = source_bucket
        self.dest_table = dest_table
        self.aws_conn_id = aws_conn_id
        self.mssql_conn_id = mssql_conn_id
        self.mssql_database = mssql_database
        self.delimiter = delimiter
        self.replace = replace
        self.truncate = truncate

    def reorder_csv(self, csv_file, ordered_file, columns):
        with open(csv_file, "r", encoding="utf-8") as infile, open(
            ordered_file, "w", encoding="utf-8"
        ) as ordered:
            writer = csv.DictWriter(
                ordered,
                fieldnames=columns,
                delimiter=self.delimiter,
                lineterminator="\n",
            )
            for row in csv.DictReader(infile):
                writer.writerow(
                    {
                        key: value.replace(self.delimiter, self.replace)
                        for key, value in row.items()
                    }
                )

    def get_column_list(self, format_file_name):
        col_list = []
        with open(format_file_name, "r") as format_file:
            # Skip version number in first line
            format_file.readline()
            # Skip num_cols number in second line
            num_cols = int(format_file.readline())
            for _ in range(num_cols):
                tokens = format_file.readline().split()
                col_name = tokens[6].lower()
                col_list.append(col_name)
        return col_list

    def _truncate_table(self):
        mssql_hook = MsSqlHook(
            mssql_conn_id=self.mssql_conn_id, schema=self.mssql_database
        )
        conn = mssql_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(
            f"""
            use {self.mssql_database}
            truncate table {self.dest_table}
        """
        )
        conn.commit()
        conn.close()

    def execute(self, context):
        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)
        bcp_hook = BcpHook(mssql_conn_id=self.mssql_conn_id, schema=self.mssql_database)

        if self.truncate:
            self._truncate_table()

        with NamedTemporaryFile() as source_csv, NamedTemporaryFile("w") as format_file:
            self.log.info(f"source_csv {source_csv.name}")
            self.log.info(f"format_file {format_file.name}")
            try:
                source_obj = s3_hook.get_key(self.source_key, self.source_bucket)
                with open(source_csv.name, "wb") as opened_source_csv:
                    source_obj.download_fileobj(opened_source_csv)
            except botocore.exceptions.ClientError:
                self.log.info("Got no file from S3, skipping")
                return False
            self.log.info("Generating format file")
            bcp_hook.generate_format_file(self.dest_table, format_file, self.delimiter)
            format_file.flush()
            self.log.info("Retrieving column list")
            col_list = self.get_column_list(format_file.name)
            self.log.info(
                "Generating rearrange CSV using column list: {0}".format(col_list)
            )

            with NamedTemporaryFile() as ordered_csv:
                self.log.info(f"ordered_csv {ordered_csv.name}")
                self.log.info("Reordering csv")
                self.reorder_csv(source_csv.name, ordered_csv.name, col_list)
                self.log.info("Importing data to SQL server")
                bcp_hook.import_data(
                    format_file.name, ordered_csv.name, self.dest_table
                )


class BcpHook(DbApiHook):
    """
    Interact with Microsoft SQL Server through bcp
    """

    conn_name_attr = "mssql_conn_id"
    default_conn_name = "mssql_default"
    supports_autocommit = True

    def __init__(self, mssql_database=None, *args, **kwargs):
        super(BcpHook, self).__init__(*args, **kwargs)
        self.schema = kwargs.pop("schema", None)

    def get_conn(self):
        """
        Returns a mssql connection details object
        """
        return self.get_connection(self.mssql_conn_id)

    def run_bcp(self, cmd):
        message_cmd = cmd[:]
        password_index = message_cmd.index("-P") + 1
        message_cmd[password_index] = "***********"
        self.log.info("Running command: {0}".format(message_cmd))
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        outs, errs = proc.communicate()
        self.log.info("Output:")
        self.log.info(outs)
        self.log.info("Stderr:")
        self.log.info(errs)
        if proc.returncode != 0:
            raise Exception("Process failed: {0}".format(proc.returncode))

    def add_conn_details(self, cmd, conn):
        conn_params = [
            "-S",
            f"{conn.host},{conn.port}",
            "-U",
            conn.login,
            "-P",
            conn.password,
            "-d",
            self.schema or conn.schema,
        ]
        cmd.extend(conn_params)

    def generate_format_file(self, table_name, format_file, delimiter="\t"):
        # Generate format file first:
        conn = self.get_conn()
        cmd = [
            "bcp",
            table_name,
            "format",
            "nul",
            "-c",
            "-f",
            format_file.name,
            f"-t{delimiter}",
        ]
        self.add_conn_details(cmd, conn)
        self.run_bcp(cmd)

    def import_data(self, format_file, data_file, table_name):
        conn = self.get_conn()
        cmd = ["bcp", table_name, "in", data_file, "-f", format_file, "-m", "1"]
        self.add_conn_details(cmd, conn)
        self.run_bcp(cmd)


class CreatePrefixTableMsSqlOperator(MsSqlOperator):
    @apply_defaults
    def __init__(
        self,
        schema,
        table_prefix,
        database,
        column_names,
        autocommit=True,
        *args,
        **kwargs,
    ):
        self._table_prefix = table_prefix
        self._schema = schema
        self.autocommit = autocommit
        if isinstance(column_names, dict):
            column_types = {k: v for k, v in column_names.items()}
        else:
            column_types = {k: "[nvarchar](max)" for k in column_names}

        for key in column_types.keys():
            if "[" not in key and "]" not in key:
                column_types[f"[{key}]"] = column_types.pop(key)

        types = ",\n".join(
            f"\t{variable} {type}" for variable, type in column_types.items()
        )

        kwargs.update(
            params=dict(schema=schema, table_prefix=table_prefix, types=types),
            sql="""
                IF OBJECT_ID('{{ params.schema }}.{{ params.table_prefix }}_{{ macros.date.format_nodash(execution_date) }}', 'U') IS NOT NULL
                DROP TABLE [{{ params.schema }}].[{{ params.table_prefix }}_{{ macros.date.format_nodash(execution_date) }}];

                CREATE TABLE [{{ params.schema }}].[{{ params.table_prefix }}_{{ macros.date.format_nodash(execution_date) }}](
                    {{ params.types }}
                );
            """,
        )

        super().__init__(autocommit=self.autocommit, *args, **kwargs)

    def execute(self, context):
        super().execute(context)
        return f"{ self._schema }.{ self._table_prefix }_{ macros.date.format_nodash(context['execution_date']) }"


class DropPrefixTableMsSqlOperator(MsSqlOperator):
    @apply_defaults
    def __init__(
        self, schema, table_prefix, database, autocommit=True, *args, **kwargs
    ):
        self.autocommit = autocommit

        kwargs.update(
            params=dict(schema=schema, table_prefix=table_prefix),
            sql="""
                DROP TABLE [{{ params.schema }}].[{{ params.table_prefix }}_{{ macros.date.format_nodash(execution_date) }}];
            """,
        )

        super().__init__(autocommit=self.autocommit, *args, **kwargs)


class MsSqlUtils(AirflowPlugin):
    name = "mssql_utils"
    operators = [
        MsSQLToS3Operator,
        MsSqlOperator,
        S3ToMsSqlOverBcpOperator,
        CreatePrefixTableMsSqlOperator,
        DropPrefixTableMsSqlOperator,
    ]
    hooks = [MsSqlHook, BcpHook]
    executors = []
    macros = []
    admin_views = []
