import ftplib
from airflow.contrib.hooks.ftp_hook import FTPHook
from airflow.plugins_manager import AirflowPlugin
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from tempfile import NamedTemporaryFile
import os
from airflow.hooks.S3_hook import S3Hook
import time
import random
from multiprocessing.dummy import Pool


class FTP_TLS_IgnoreHost(ftplib.FTP_TLS):
    def makepasv(self):
        _, port = super().makepasv()
        return self.host, port


class FTPSNoHostHook(FTPHook):
    def get_conn(self):
        """
        Returns a FTPS connection object.
        """
        if self.conn is None:
            params = self.get_connection(self.ftp_conn_id)
            pasv = params.extra_dejson.get("passive", True)

            if params.port:
                FTP_TLS_IgnoreHost.port = params.port

            self.conn = FTP_TLS_IgnoreHost(params.host, params.login, params.password)
            self.conn.set_pasv(pasv)
            self.conn.prot_p()

        return self.conn


class BulkFTPToS3Operator(BaseOperator):
    template_fields = ("ftp_path", "dest_path")

    @apply_defaults
    def __init__(
        self,
        ftp_path,
        ftp_conn_id,
        dest_path,
        dest_bucket,
        aws_conn_id,
        ftp_hook=FTPHook,
        workers=4,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.ftp_path = ftp_path
        self.ftp_conn_id = ftp_conn_id
        self.dest_path = dest_path
        self.dest_bucket = dest_bucket
        self.aws_conn_id = aws_conn_id
        self.ftp_hook = ftp_hook
        self.workers = workers

    def execute(self, context):
        self.log.info("Going to start Bulk ftp to s3 operator")
        ftp_hook = self.ftp_hook(self.ftp_conn_id)
        ftp_client = ftp_hook.get_conn()
        s3_hook = S3Hook(self.aws_conn_id)
        list_dir = ftp_client.nlst(self.ftp_path)

        if len(list_dir) < 1:
            self.log.info("Got no files to process. Skipping")
            return False

        self.log.info(f"Got {len(list_dir)} files to move")
        file_path_list = []
        temp_files = []
        self.log.info("Downloading files")
        for file_path in list_dir:
            file_path_list.append(file_path)
            file_name = file_path.split("/")[-1]
            s3_key = str(os.path.join(self.dest_path, file_name))
            file_metadata = {"ftp": NamedTemporaryFile("wb"), "s3_key": s3_key}
            for i in range(0, 5):
                try:
                    ftp_client.retrbinary(
                        f"RETR {file_path}", file_metadata["ftp"].write
                    )
                    file_metadata["ftp"].flush()
                    temp_files.append(file_metadata)
                    break
                except Exception:
                    self.log.info(
                        f"Got no response from server, waiting for next try number {(i + 1)}"
                    )
                    if i < 4:
                        time.sleep(2 ** i + random.random())
                        ftp_client = self.ftp_hook(self.ftp_conn_id).get_conn()
                    else:
                        raise

        self.log.info(f"Uploading to S3 with {self.workers} workers")
        with Pool(self.workers) as pool:
            pool.starmap(
                s3_hook.load_file,
                [
                    (x["ftp"].name, x["s3_key"], self.dest_bucket, True, False)
                    for x in temp_files
                ],
            )

        ftp_client.close()
        self.log.info("Finished executing Bulk ftp to s3 operator")
        return file_path_list


class FTPUtils(AirflowPlugin):
    name = "ftp_utils"
    operators = [BulkFTPToS3Operator]
    hooks = [FTPSNoHostHook]
    executors = []
    macros = []
    admin_views = []
