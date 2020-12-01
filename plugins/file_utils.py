from tempfile import NamedTemporaryFile
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
import pandas
import csv
from unidecode import unidecode
import os
import itertools
import json
import ast


class ExcelToCSVOperator(BaseOperator):
    template_fields = ("excel_key", "csv_key")

    @apply_defaults
    def __init__(
        self,
        excel_key,
        excel_bucket,
        csv_key,
        csv_bucket,
        aws_conn_id,
        store_file_name=False,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.excel_key = excel_key
        self.excel_bucket = excel_bucket
        self.csv_key = csv_key
        self.csv_bucket = csv_bucket
        self.aws_conn_id = aws_conn_id
        self.store_file_name = store_file_name

    def execute(self, context):
        self.log.info("Starting executing Excel to CSV Operator")
        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)

        self.log.info("Downloading S3 File")
        with NamedTemporaryFile() as source_excel:
            source_obj = s3_hook.get_key(self.excel_key, self.excel_bucket)
            with open(source_excel.name, "wb") as opened_source_excel:
                source_obj.download_fileobj(opened_source_excel)

            self.log.info("Tranforming xlsx to csv")
            with NamedTemporaryFile() as csv_file:
                with open(csv_file.name, "w") as opened_csv_file:
                    data_frame = pandas.read_excel(source_excel.name)

                    if self.store_file_name:
                        data_frame["file_name"] = self.excel_key.split("/")[-1]

                    data_frame.to_csv(opened_csv_file, index=False)
                    opened_csv_file.flush()

                self.log.info("Uploading csv to S3")
                with open(csv_file.name) as opened_csv_file:
                    if len(opened_csv_file.readlines()) <= 1:
                        self.log.info("CSV Returned no Rows. Skipping")
                        return False

                s3_hook.load_file(
                    filename=csv_file.name,
                    key=self.csv_key,
                    bucket_name=self.csv_bucket,
                    replace=True,
                )
                self.log.info("Finished executing ExcelToCSVOperator")

                return True


class BulkExcelToCSVOperator(BaseOperator):
    template_fields = ("excel_path", "csv_path")

    @apply_defaults
    def __init__(
        self,
        excel_path,
        excel_bucket,
        csv_path,
        csv_bucket,
        aws_conn_id,
        store_file_name=False,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.excel_path = excel_path
        self.excel_bucket = excel_bucket
        self.csv_path = csv_path
        self.csv_bucket = csv_bucket
        self.aws_conn_id = aws_conn_id
        self.store_file_name = store_file_name

    def execute(self, context):
        self.log.info("Going to start Bulk Excel to CSV Operator")
        s3_hook = S3Hook(self.aws_conn_id)
        s3_files = s3_hook.list_keys(
            bucket_name=self.excel_bucket, prefix=self.excel_path
        )

        self.log.info(f"Got {len(s3_files)} files to convert")
        for key in s3_files:
            file_name = key.split("/")[-1][:-3]
            file_name = f"{file_name}.csv"
            file_path = os.path.join(self.csv_path, file_name)

            excel_operator = ExcelToCSVOperator(
                task_id=context["task"].task_id,
                excel_key=key,
                excel_bucket=self.excel_bucket,
                csv_key=file_path,
                csv_bucket=self.csv_bucket,
                aws_conn_id=self.aws_conn_id,
                store_file_name=self.store_file_name,
            )
            excel_operator.execute(context=context)

        self.log.info("Finished executing Bulk excel to csv operator")
        return s3_files


class StandardizeCSVHeaderOperator(BaseOperator):
    template_fields = ("source_key", "dest_key")

    @apply_defaults
    def __init__(
        self,
        source_key,
        source_bucket,
        aws_conn_id,
        dest_key=None,
        dest_bucket=None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

        if dest_bucket is None:
            self.dest_bucket = source_bucket
        else:
            self.dest_bucket = dest_bucket

        if dest_key is None:
            self.dest_key = source_key
        else:
            self.dest_key = dest_key

        self.source_key = source_key
        self.source_bucket = source_bucket
        self.aws_conn_id = aws_conn_id

    def execute(self, context):
        self.log.info("Starting executing Standardize CSV Header Operator")
        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)

        self.log.info("Downloading S3 File")
        with NamedTemporaryFile() as source_csv:
            source_obj = s3_hook.get_key(self.source_key, self.source_bucket)
            with open(source_csv.name, "wb") as opened_source_csv:
                source_obj.download_fileobj(opened_source_csv)

            self.log.info("Building new header")
            with open(source_csv.name, "r") as opened_source_csv:
                reader = csv.reader(opened_source_csv)
                header = next(reader)
                self.log.info(f"Got header {header}")
                header = [
                    unidecode(x).replace(" ", "_").replace(".", "").lower()
                    for x in header
                ]

            self.log.info("Generating new csv")
            with NamedTemporaryFile() as dest_csv:
                with open(source_csv.name, "r") as opened_source_csv, open(
                    dest_csv.name, "w"
                ) as opened_dest_csv:
                    reader = csv.reader(opened_source_csv)
                    writer = csv.writer(opened_dest_csv)

                    next(reader)
                    writer.writerow(header)
                    writer.writerows(reader)
                    opened_dest_csv.flush()

                self.log.info("Uploading csv to S3")
                with open(dest_csv.name) as opened_dest_csv:
                    if len(opened_dest_csv.readlines()) <= 1:
                        self.log.info("CSV Returned no Rows. Skipping")
                        return False

                s3_hook.load_file(
                    filename=dest_csv.name,
                    key=self.dest_key,
                    bucket_name=self.dest_bucket,
                    replace=True,
                )
                self.log.info("Finised Standardize CSV Header Operator")


class ConcatenateCSVOperator(BaseOperator):
    template_fields = ("source_path", "dest_key")

    @apply_defaults
    def __init__(
        self,
        source_path,
        source_bucket,
        dest_key,
        aws_conn_id,
        dest_bucket=None,
        headers=None,
        encoding="utf-8",
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        if dest_bucket is None:
            self.dest_bucket = source_bucket
        else:
            self.dest_bucket = dest_bucket
        self.source_path = source_path
        self.dest_key = dest_key
        self.aws_conn_id = aws_conn_id
        self.source_bucket = source_bucket
        self.headers = headers
        self.encoding = encoding

    def execute(self, context):
        self.log.info("Going to start Concatenate csv operator")
        s3_hook = S3Hook(self.aws_conn_id)

        s3_files = s3_hook.list_keys(
            bucket_name=self.source_bucket, prefix=self.source_path
        )
        s3_files = [x for x in s3_files if x[-3:] == "csv"]
        self.log.info(f"Got {len(s3_files)} to concatenate")

        source_obj = s3_hook.get_key(s3_files[0], self.source_bucket)
        with NamedTemporaryFile() as headers_file:
            with open(headers_file.name, "wb") as opened_headers_file:
                source_obj.download_fileobj(opened_headers_file)

            if self.headers:
                headers = self.headers
            else:
                with open(headers_file.name, "r") as opened_headers_file:
                    reader = csv.reader(opened_headers_file)
                    headers = next(reader)
        self.log.info(f"Got header {headers}")

        with NamedTemporaryFile() as dest_file:
            with open(dest_file.name, "w") as opened_dest_file:
                writer = csv.DictWriter(opened_dest_file, fieldnames=headers)
                writer.writeheader()

                self.log.info("Iterating over s3 keys")
                for s3_key in s3_files:
                    source_obj = s3_hook.get_key(s3_key, self.source_bucket)
                    with NamedTemporaryFile() as source_file:
                        with open(source_file.name, "wb") as opened_source_file:
                            source_obj.download_fileobj(opened_source_file)

                        with open(
                            source_file.name, "r", encoding=self.encoding
                        ) as opened_source_file:
                            reader = csv.DictReader(
                                opened_source_file, fieldnames=headers
                            )
                            writer.writerows(reader)
                opened_dest_file.flush()

            self.log.info("Uploading csv to S3")
            with open(dest_file.name) as opened_dest_file:
                if len(opened_dest_file.readlines()) <= 1:
                    self.log.info("CSV Returned no Rows. Skipping")
                    return False

            s3_hook.load_file(
                filename=dest_file.name,
                key=self.dest_key,
                bucket_name=self.dest_bucket,
                replace=True,
            )
            self.log.info("Finised executing Concatenate CSV Operator")


class BulkCopyS3Operator(BaseOperator):
    template_fields = ("s3_keys", "dest_path")

    @apply_defaults
    def __init__(
        self,
        aws_conn_id,
        s3_keys,
        source_bucket,
        dest_bucket,
        dest_path,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.source_bucket = source_bucket
        self.dest_bucket = dest_bucket
        self.dest_path = dest_path
        self.s3_keys = s3_keys

    def execute(self, context):
        if isinstance(self.s3_keys, str):
            self.s3_keys = ast.literal_eval(self.s3_keys)

        self.log.info("Going to start Bulk Copy S3 Operator")
        s3_hook = S3Hook(self.aws_conn_id)
        s3_conn = s3_hook.get_conn()

        self.log.info(f"Got {len(self.s3_keys)} keys to move")
        for key in self.s3_keys:
            self.log.info(f"Going to copy {key}")
            copy_source = {"Bucket": self.source_bucket, "Key": key}
            source_file = key.split("/")[-1]
            dest_key = os.path.join(self.dest_path, source_file)
            s3_conn.copy(copy_source, self.dest_bucket, dest_key)
        self.log.info("Finised executing Bulk Copy S3 Operator")


class CSVToJsonOperator(BaseOperator):
    template_fields = ("csv_key", "json_key")

    @apply_defaults
    def __init__(
        self,
        aws_conn_id,
        csv_key,
        csv_bucket,
        json_key,
        json_bucket,
        python_callable,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.csv_key = csv_key
        self.csv_bucket = csv_bucket
        self.json_key = json_key
        self.json_bucket = json_bucket
        self.python_callable = python_callable

    def execute(self, context):
        self.log.info("Going to execute CSV to Json Operator")
        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)

        self.log.info("Downloading S3 File")
        with NamedTemporaryFile() as csv_file:
            source_obj = s3_hook.get_key(self.csv_key, self.csv_bucket)
            with open(csv_file.name, "wb") as opened_csv_file:
                source_obj.download_fileobj(opened_csv_file)

            with open(csv_file.name, "r") as opened_csv_file:
                reader = csv.DictReader(opened_csv_file)
                json_data = self.python_callable(reader)

                try:
                    first_row = next(json_data)
                except StopIteration:
                    self.log.info("Callable didn't return any rows")
                    return False

                self.log.info("Uploading to S3")
                rows = itertools.chain([first_row], json_data)
                with NamedTemporaryFile() as final_file:
                    with open(
                        final_file.name, "w", encoding="utf-8"
                    ) as opened_final_file:
                        for row in rows:
                            opened_final_file.write(json.dumps(row, ensure_ascii=False))
                            opened_final_file.write("\n")
                        opened_final_file.flush()

                    s3_hook.load_file(
                        filename=final_file.name,
                        key=self.json_key,
                        bucket_name=self.json_bucket,
                        replace=True,
                    )
                self.log.info("Finished executing CSV to JSON Operator")
                return True


class FileUtils(AirflowPlugin):
    name = "file_utils"
    operators = [
        ExcelToCSVOperator,
        StandardizeCSVHeaderOperator,
        ConcatenateCSVOperator,
        BulkExcelToCSVOperator,
        BulkCopyS3Operator,
        CSVToJsonOperator,
    ]
    hooks = []
    executors = []
    macros = []
    admin_views = []
