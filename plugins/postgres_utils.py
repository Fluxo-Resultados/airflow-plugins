import csv
from tempfile import NamedTemporaryFile
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
from airflow.operators.postgres_operator import PostgresOperator
from airflow import macros
from contextlib import closing
import json
import uuid


class S3ToPostgresOperator(BaseOperator):
    """
    Insert data into generic Postgres database with copy_from method from psycopg2
    """

    template_fields = ("source_key", "dest_table")

    @apply_defaults
    def __init__(
        self,
        source_key,
        source_bucket,
        dest_table,
        aws_conn_id="aws_default",
        postgres_conn_id="postgres_default",
        postgres_database=None,
        copy_sep=",",
        copy_null="",
        is_json=False,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.source_key = source_key
        self.source_bucket = source_bucket
        self.dest_table = dest_table
        self.aws_conn_id = aws_conn_id
        self.postgres_conn_id = postgres_conn_id
        self.postgres_database = postgres_database
        self.copy_sep = copy_sep
        self.copy_null = copy_null
        self.is_json = is_json

    def execute(self, context):
        self.log.info("Getting Connections")
        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)
        postgres_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        postgres_conn = postgres_hook.get_conn()
        postgres_cursor = postgres_conn.cursor()

        self.log.info("Downloading S3 File")
        with NamedTemporaryFile() as source_csv:
            source_obj = s3_hook.get_key(self.source_key, self.source_bucket)
            with open(source_csv.name, "wb") as opened_source_csv:
                source_obj.download_fileobj(opened_source_csv)
                opened_source_csv.flush()

            with open(source_csv.name, "r") as opened_csv:
                if sum(1 for row in opened_csv) <= 1:
                    self.log.info("CSV returned no row(s). Skipping.")
                    return False

            self.log.info("Replacing special chars")
            with open(source_csv.name, "r+") as opened_source_csv, NamedTemporaryFile(
                "w"
            ) as sanitized_csv:
                if self.is_json:
                    for row in opened_source_csv:
                        sanitized_csv.write(row.replace("\\", "").replace("'", ""))
                else:
                    csv_reader = csv.reader(opened_source_csv, delimiter=self.copy_sep)
                    csv_writer = csv.writer(sanitized_csv, delimiter=self.copy_sep)
                    for row in csv_reader:
                        csv_writer.writerow(
                            [
                                x.replace("\\", "").replace("'", "").replace("\n", " ")
                                for x in row
                            ]
                        )
                sanitized_csv.flush()

                self.log.info("Copying File to database")
                with open(sanitized_csv.name, "r") as opened_source_csv:
                    csv_reader = csv.reader(opened_source_csv, delimiter=self.copy_sep)
                    column_list = next(csv_reader)
                    self.log.info(f"Got columns: {column_list}")
                    postgres_cursor.copy_from(
                        opened_source_csv,
                        self.dest_table,
                        sep=self.copy_sep,
                        columns=column_list,
                        null=self.copy_null,
                    )
                    postgres_conn.commit()


class CreatePrefixTablePostgresOperator(PostgresOperator):
    @apply_defaults
    def __init__(
        self,
        schema,
        table_prefix,
        database,
        column_types,
        autocommit=True,
        *args,
        **kwargs,
    ):
        self._table_prefix = table_prefix
        self._schema = schema
        self.autocommit = autocommit
        self.column_types = column_types

        types = ",\n".join(
            f'\t"{variable}" {type}' for variable, type in column_types.items()
        )

        kwargs.update(
            params=dict(
                schema=schema, database=database, table_prefix=table_prefix, types=types
            ),
            sql="""
                DROP TABLE IF EXISTS "{{ params.schema }}"."{{ params.table_prefix }}_{{ macros.date.format_nodash(execution_date).lower() }}";

                CREATE TABLE "{{ params.schema }}"."{{ params.table_prefix }}_{{ macros.date.format_nodash(execution_date).lower() }}"(
                    {{ params.types }}
                );
            """,
        )

        super().__init__(autocommit=self.autocommit, *args, **kwargs)

    def execute(self, context):
        super().execute(context)
        return f"{ self._schema }.{ self._table_prefix }_{ macros.date.format_nodash(context['execution_date']).lower() }"


class DropPrefixTablePostgresOperator(PostgresOperator):
    @apply_defaults
    def __init__(
        self, schema, table_prefix, database, autocommit=True, *args, **kwargs
    ):
        self.autocommit = autocommit

        kwargs.update(
            params=dict(schema=schema, database=database, table_prefix=table_prefix),
            sql="""
                DROP TABLE IF EXISTS "{{ params.schema }}"."{{ params.table_prefix }}_{{ macros.date.format_nodash(execution_date).lower() }}";
            """,
        )

        super().__init__(autocommit=self.autocommit, *args, **kwargs)


class PostgresXcomOperator(BaseOperator):
    template_fields = ("sql",)

    @apply_defaults
    def __init__(
        self, postgres_conn_id, sql, database, python_callable=None, *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.sql = sql
        self.database = database
        self.python_callable = python_callable

    def execute(self, context):
        self.log.info("Going To Start Postgres Xcom Operator")
        postgres_hook = PostgresHook(self.postgres_conn_id)
        conn = postgres_hook.get_conn()

        cursor = conn.cursor()
        self.log.info(f"Going to run:\n{self.sql}")
        cursor.execute(self.sql)
        results = cursor.fetchall()
        if self.python_callable:
            results = self.python_callable(results)
        if results:
            self.log.info("Got results, pushing xcom")
            return results
        else:
            self.log.info("No Results, pushing false")
            return False


class PostgresToS3Operator(BaseOperator):
    template_fields = ("sql", "dest_key")
    template_ext = (".sql",)

    @apply_defaults
    def __init__(
        self,
        sql,
        dest_key,
        dest_bucket,
        sql_parameters=None,
        postgres_conn_id="postgres_default",
        aws_conn_id="aws_default",
        persist_header=True,
        is_json=False,
        iter_size=10000,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.aws_conn_id = aws_conn_id
        self.sql = sql
        self.dest_key = dest_key
        self.dest_bucket = dest_bucket
        self.sql_parameters = sql_parameters
        self.persist_header = persist_header
        self.is_json = is_json
        self.iter_size = iter_size

    def execute(self, context):
        self.log.info("Start executing PostgresToS3Operator")
        psql_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)
        with closing(psql_hook.get_conn()) as connection:
            with connection, connection.cursor(str(uuid.uuid1())) as cursor:
                cursor.itersize = self.iter_size
                self.log.info(f"Going to execute query {self.sql}")
                cursor.execute(self.sql, self.sql_parameters)

                self.log.info("Writing data")
                i = 0
                with NamedTemporaryFile("w", encoding="utf-8") as temp_file:
                    if self.is_json:
                        for row in cursor:
                            i += 1
                            temp_file.write(json.dumps(row[0]))
                            temp_file.write("\n")
                    else:
                        writer = csv.writer(temp_file, lineterminator="\n")
                        for row in cursor:
                            i += 1
                            writer.writerow(row)
                    temp_file.flush()

                    if i < 1:
                        self.log.info("Got no rows from query. Skipping")
                        return False

                    columns = [description[0] for description in cursor.description]
                    self.log.info(f"Got columns {columns}")

                    with NamedTemporaryFile("w", encoding="utf-8") as file:
                        if self.persist_header:
                            writer = csv.writer(file, lineterminator="\n")
                            writer.writerow(columns)
                            file.flush()
                        with open(
                            temp_file.name, "r", encoding="utf-8"
                        ) as opened_temp_file:
                            for row in opened_temp_file:
                                file.write(row)
                            file.flush()

                        s3_hook.load_file(
                            filename=file.name,
                            key=self.dest_key,
                            bucket_name=self.dest_bucket,
                            replace=True,
                        )
                        return True


class S3ToPostgresPlugin(AirflowPlugin):
    name = "postgres_utils"
    operators = [
        S3ToPostgresOperator,
        CreatePrefixTablePostgresOperator,
        DropPrefixTablePostgresOperator,
        PostgresXcomOperator,
        PostgresToS3Operator,
    ]
    hooks = []
    executors = []
    macros = []
    admin_views = []
