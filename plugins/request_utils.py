import csv
from urllib.parse import urljoin
import itertools
from tempfile import NamedTemporaryFile
from requests import request

from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
from airflow.hooks.base_hook import BaseHook


def default(argument):
    if argument is None:
        return {}

    return argument


class RequestToS3Operator(BaseOperator):
    template_fields = ("json_template", "params_template", "path", "dest_key")
    template_ext = (".sql",)

    @apply_defaults
    def __init__(
        self,
        method,
        path,
        python_callable,
        dest_bucket,
        dest_key,
        http_conn_id,
        params=None,
        params_template=None,
        json=None,
        json_template=None,
        aws_conn_id="aws_default",
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        params = default(params)
        params_template = default(params_template)
        json = default(json)
        json_template = default(json_template)

        self.aws_conn_id = aws_conn_id
        self.http_conn_id = http_conn_id
        self.path = path
        self.method = method
        self.params = params
        self.params_template = params_template
        self.json = json
        self.json_template = json_template
        self.python_callable = python_callable
        self.dest_key = dest_key
        self.dest_bucket = dest_bucket

    def execute(self, context):
        conn = BaseHook.get_connection(self.http_conn_id)
        url = urljoin(conn.host, self.path)
        json = {**self.json, **self.json_template}
        params = {**self.params, **self.params_template}

        self.log.info(
            f"""
            Goint to query {url} with:

            params: {params}
            json: {json}
        """
        )

        extras = conn.extra_dejson

        json = {**extras.get("json", {}), **json}
        headers = {**extras.get("headers", {})}
        params = {**extras.get("params", {}), **params}

        request_kwargs = {"json": json, "headers": headers, "params": params}
        request_kwargs = {k: v for k, v in request_kwargs.items() if v != {}}
        if conn.login and conn.password:
            request_kwargs = {**request_kwargs, "auth": (conn.login, conn.password)}

        response = request(method=self.method, url=url, **request_kwargs)

        self.log.info(f"Request returned with {response}")
        response.raise_for_status()

        data = response.json()
        self.log.info("Got response")

        self.log.info("Goint to pass data to python_callable function")
        rows = self.python_callable(data)
        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)

        try:
            first_row = next(rows)
        except StopIteration:
            self.log.info("Callable didn't return any rows")
            return False

        columns = first_row.keys()
        self.log.info(f"Got columns {columns}")
        rows = itertools.chain([first_row], rows)

        with NamedTemporaryFile("w", encoding="utf-8") as file:
            writer = csv.DictWriter(file, fieldnames=columns, lineterminator="\n")

            writer.writeheader()
            writer.writerows(rows)
            file.flush()

            s3_hook.load_file(
                filename=file.name,
                key=self.dest_key,
                bucket_name=self.dest_bucket,
                replace=True,
            )

        return True


class RequestToS3(AirflowPlugin):
    name = "request_utils"
    operators = [RequestToS3Operator]
    hooks = []
    executors = []
    macros = []
    admin_views = []
