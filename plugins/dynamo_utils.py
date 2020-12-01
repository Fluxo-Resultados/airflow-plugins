from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_dynamodb_hook import AwsDynamoDBHook
from tempfile import NamedTemporaryFile
import json
from boto3.dynamodb.conditions import Key
from decimal import Decimal


class S3ToDynamoDBOperator(BaseOperator):
    template_fields = ("s3_key",)

    @apply_defaults
    def __init__(
        self,
        s3_conn_id,
        dynamodb_conn_id,
        s3_key,
        s3_bucket,
        table_name,
        table_keys=None,
        region_name=None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.s3_conn_id = s3_conn_id
        self.dynamodb_conn_id = dynamodb_conn_id
        self.s3_key = s3_key
        self.s3_bucket = s3_bucket
        self.table_name = table_name
        self.table_keys = table_keys
        self.region_name = region_name

    def clean_empty(self, d):
        if not isinstance(d, (dict, list)):
            return d
        if isinstance(d, list):
            return [v for v in (self.clean_empty(v) for v in d) if v]
        return {k: v for k, v in ((k, self.clean_empty(v)) for k, v in d.items()) if v}

    def execute(self, context):
        self.log.info("Going to start S3 to Dynamo operator")
        dynamo_hook = AwsDynamoDBHook(
            aws_conn_id=self.dynamodb_conn_id,
            table_keys=self.table_keys,
            table_name=self.table_name,
            region_name=self.region_name,
        )
        s3_hook = S3Hook(self.s3_conn_id)

        self.log.info("Downloading s3 file")
        source_obj = s3_hook.get_key(self.s3_key, self.s3_bucket)
        with NamedTemporaryFile() as source_file:
            with open(source_file.name, "wb") as opened_source_file:
                source_obj.download_fileobj(opened_source_file)

            self.log.info("Writing file to Dynamo")
            with open(source_file.name, "r") as opened_source_file:
                reader = opened_source_file.readlines()
                json_data = [
                    self.clean_empty(json.loads(x, parse_float=Decimal)) for x in reader
                ]
                dynamo_hook.write_batch_data(json_data)

        self.log.info("Finished S3 to Dynamo operator")
        return True


class XcomDynamoDBKeyOperator(BaseOperator):
    template_fields = ("partition_key_condition",)

    @apply_defaults
    def __init__(
        self,
        dynamodb_conn_id,
        table_name,
        partition_key_condition,
        region_name,
        return_key,
        table_keys=None,
        sort_key_condition=None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.dynamodb_conn_id = dynamodb_conn_id
        self.table_name = table_name
        self.partition_key_condition = partition_key_condition
        self.table_keys = table_keys
        self.region_name = region_name
        self.return_key = return_key
        self.sort_key_condition = sort_key_condition

    def execute(self, context):
        self.log.info("Going to start Xcom DynamoDB Key Operator")
        dynamo_hook = AwsDynamoDBHook(
            aws_conn_id=self.dynamodb_conn_id,
            table_keys=self.table_keys,
            table_name=self.table_name,
            region_name=self.region_name,
        )
        condition = Key(self.partition_key_condition[0]).eq(
            self.partition_key_condition[1]
        )
        if self.sort_key_condition:
            condition = condition & self.sort_key_condition

        conn = dynamo_hook.get_conn()
        table = conn.Table(self.table_name)
        response = table.query(KeyConditionExpression=condition)
        if response["Items"]:
            self.log.info("Got response, validating")
            return [x[self.return_key] for x in response["Items"]]
        else:
            self.log.info("Responde didn't return results")
            return False


class FileUtils(AirflowPlugin):
    name = "dynamo_utils"
    operators = [S3ToDynamoDBOperator, XcomDynamoDBKeyOperator]
    hooks = []
    executors = []
    macros = []
    admin_views = []
