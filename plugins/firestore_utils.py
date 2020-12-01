import json
import firebase_admin
from firebase_admin import credentials, firestore
from tempfile import NamedTemporaryFile
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
from airflow.hooks.base_hook import BaseHook


class FirestoreHook(BaseHook):
    def __init__(self, google_conn_id):
        self.google_conn_id = google_conn_id
        conn = self.get_connection(self.google_conn_id).extra_dejson.get("credentials")
        with NamedTemporaryFile("w") as cred_file:
            cred_file.write(json.dumps(conn))
            cred_file.flush()
            cred = credentials.Certificate(cred_file.name)
            firebase_admin.initialize_app(cred)
            self._client = firestore.client()

    def get_conn(self):
        return self._client


class S3ToFirestoreOperator(BaseOperator):
    template_fields = ("s3_key",)

    @apply_defaults
    def __init__(
        self,
        aws_conn_id,
        s3_key,
        s3_bucket,
        collection,
        google_conn_id,
        action="set",
        id_column="id",
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.s3_key = s3_key
        self.s3_bucket = s3_bucket
        self.collection = collection
        self.google_conn_id = google_conn_id
        self.action = action
        self.id_column = id_column

    def execute(self, context):
        if self.action not in ["set", "delete"]:
            raise TypeError("Action is not set or delete, failing")

        self.log.info("Going to start S3 to Firestore Operator")
        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)
        firestore_hook = FirestoreHook(self.google_conn_id)

        self.log.info("Downloading s3 file")
        source_obj = s3_hook.get_key(self.s3_key, self.s3_bucket)
        with NamedTemporaryFile() as source_file:
            with open(source_file.name, "wb") as opened_source_file:
                source_obj.download_fileobj(opened_source_file)

            self.log.info("Going to insert data into firestore")
            db = firestore_hook.get_conn()
            collection = db.collection(self.collection)
            idx = 0
            batch = db.batch()
            with open(source_file.name, "r") as opened_source_file:
                for record in opened_source_file:
                    if idx % 500 == 0:
                        if idx > 0:
                            self.log.info(f"Commiting with idx={idx}")
                            batch.commit()
                        batch = db.batch()
                    idx += 1
                    json_data = json.loads(record)
                    ref = collection.document(str(json_data[self.id_column]))
                    getattr(batch, self.action)(ref, json_data)
                if idx % 500 != 0:
                    self.log.info(f"Commiting with idx={idx}")
                    batch.commit()
        self.log.info("Done")
        return True


class FirestoreUtils(AirflowPlugin):
    name = "firestore_utils"
    operators = [S3ToFirestoreOperator]
    hooks = [FirestoreHook]
    executors = []
    macros = []
    admin_views = []
