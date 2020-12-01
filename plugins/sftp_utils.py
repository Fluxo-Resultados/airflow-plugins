from airflow.models import BaseOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.contrib.hooks.ssh_hook import SSHHook
from tempfile import NamedTemporaryFile
from airflow.utils.decorators import apply_defaults
import os
from airflow.plugins_manager import AirflowPlugin
from airflow.contrib.hooks.sftp_hook import SFTPHook
import ast
from multiprocessing.dummy import Pool
import time
import random
from airflow.hooks.base_hook import BaseHook
import subprocess


class BulkS3ToSFTPOperator(BaseOperator):
    template_fields = ("s3_path", "sftp_path")

    @apply_defaults
    def __init__(
        self,
        s3_bucket,
        s3_path,
        sftp_path,
        sftp_conn_id="ssh_default",
        s3_conn_id="aws_default",
        *args,
        **kwargs,
    ):
        super(BulkS3ToSFTPOperator, self).__init__(*args, **kwargs)
        self.sftp_conn_id = sftp_conn_id
        self.sftp_path = sftp_path
        self.s3_bucket = s3_bucket
        self.s3_path = s3_path
        self.s3_conn_id = s3_conn_id

    def execute(self, context):
        ssh_hook = SSHHook(ssh_conn_id=self.sftp_conn_id)
        s3_hook = S3Hook(self.s3_conn_id)

        s3_files = s3_hook.list_keys(bucket_name=self.s3_bucket, prefix=self.s3_path)

        s3_client = s3_hook.get_conn()
        sftp_client = ssh_hook.get_conn().open_sftp()

        for key in s3_files:
            file_name = key.split("/")[-1]

            with NamedTemporaryFile("w") as f:
                s3_client.download_file(self.s3_bucket, key, f.name)
                sftp_client.put(f.name, os.path.join(self.sftp_path, file_name))


class BulkSFTPToS3Operator(BaseOperator):
    template_fields = ("sftp_path", "dest_path")

    @apply_defaults
    def __init__(
        self,
        sftp_path,
        sftp_conn_id,
        dest_path,
        dest_bucket,
        aws_conn_id,
        workers=4,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.sftp_path = sftp_path
        self.sftp_conn_id = sftp_conn_id
        self.dest_path = dest_path
        self.dest_bucket = dest_bucket
        self.aws_conn_id = aws_conn_id
        self.workers = workers

    def execute(self, context):
        self.log.info("Going to start Bulk sftp to s3 operator")
        sftp_hook = SFTPHook(ftp_conn_id=self.sftp_conn_id)
        sftp_hook.no_host_key_check = True
        list_dir = sftp_hook.list_directory(self.sftp_path)

        if len(list_dir) < 1:
            self.log.info("Got no files to process. Skipping")
            return False

        self.log.info(f"Got {len(list_dir)} files to move")
        temp_files = []
        file_path_list = []
        ssh_hook = SSHHook(ssh_conn_id=self.sftp_conn_id)
        sftp_client = ssh_hook.get_conn().open_sftp()
        s3_hook = S3Hook(self.aws_conn_id)
        for file_name in list_dir:
            file_path = os.path.join(self.sftp_path, file_name)
            file_path_list.append(file_path)
            s3_key = str(os.path.join(self.dest_path, file_name))
            file_metadata = {"ftp": NamedTemporaryFile("w"), "s3_key": s3_key}
            for i in range(0, 5):
                try:
                    self.log.info(f"Downloading {file_path}")
                    sftp_client.get(file_path, file_metadata["ftp"].name)
                    file_metadata["ftp"].flush()
                    temp_files.append(file_metadata)
                    break
                except Exception:
                    self.log.info(
                        f"Got no response from server, waiting for next try number {(i + 1)}"
                    )
                    if i < 4:
                        time.sleep(2 ** i + random.random())
                        sftp_client = (
                            SSHHook(ssh_conn_id=self.sftp_conn_id)
                            .get_conn()
                            .open_sftp()
                        )
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

        self.log.info("Finished executing Bulk sftp to s3 operator")
        return file_path_list


class RenameSFTPOperator(BaseOperator):
    template_fields = ("source_file", "dest_file")

    @apply_defaults
    def __init__(self, sftp_conn_id, source_file, dest_file, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sftp_conn_id = sftp_conn_id
        self.source_file = source_file
        self.dest_file = dest_file

    def execute(self, context):
        self.log.info("Going to start Rename SFTP Operator")
        sftp_hook = SFTPHook(ftp_conn_id=self.sftp_conn_id)
        sftp_hook.no_host_key_check = True
        conn = sftp_hook.get_conn()
        try:
            conn.rename(self.source_file, self.dest_file)
        except IOError:
            self.log.info("File not found, skipping")
        self.log.info("Finished executing RenameSFTPOperator")


class BulkRenameSFTPOperator(BaseOperator):
    template_fields = ("source_files", "dest_path")

    @apply_defaults
    def __init__(
        self,
        sftp_conn_id,
        dest_path,
        source_files=None,
        source_path=None,
        file_limit=None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.sftp_conn_id = sftp_conn_id
        self.source_files = source_files
        self.dest_path = dest_path
        self.source_path = source_path
        self.file_limit = file_limit

    def execute(self, context):
        self.log.info("Going to start Bulk Rename sftp operator")
        sftp_hook = SFTPHook(ftp_conn_id=self.sftp_conn_id)
        sftp_hook.no_host_key_check = True
        if self.source_files:
            if type(self.source_files) is str:
                source_files_list = ast.literal_eval(self.source_files)

        if self.source_path:
            source_files_list = sftp_hook.list_directory(self.source_path)
            source_files_list = [
                os.path.join(self.source_path, x) for x in source_files_list
            ]

        file_path_list = []
        if self.file_limit:
            source_files_list = source_files_list[: self.file_limit]
        for key in source_files_list:
            file_path = key.split("/")[-1]
            file_path = os.path.join(self.dest_path, file_path)
            self.log.info(f"Renaming {key} to {file_path}")

            conn = sftp_hook.get_conn()
            for i in range(0, 5):
                try:
                    try:
                        conn.remove(file_path)
                        print("Deleted duplicated file")
                    except IOError:
                        pass

                    conn.rename(key, file_path)
                    file_path_list.append(file_path)
                    break
                except IOError:
                    self.log.info("File not found, skipping")
                    break
                except Exception:
                    self.log.info(
                        f"Got no response from server, waiting for next try number {(i + 1)}"
                    )
                    if i < 4:
                        time.sleep(2 ** i + random.random())
                        sftp_hook = SFTPHook(ftp_conn_id=self.sftp_conn_id)
                        sftp_hook.no_host_key_check = True
                        conn = sftp_hook.get_conn()
                    else:
                        raise

        self.log.info("Finished executing Bulk Rename sftp operator")
        return file_path_list


class DeleteFileSFTPOperator(BaseOperator):
    template_fields = ("file_path",)

    @apply_defaults
    def __init__(self, sftp_conn_id, file_path, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sftp_conn_id = sftp_conn_id
        self.file_path = file_path

    def execute(self, context):
        self.log.info("Going to start delete file sftp operator")
        sftp_hook = SFTPHook(ftp_conn_id=self.sftp_conn_id)
        sftp_hook.no_host_key_check = True
        sftp_hook.delete_file(self.file_path)
        self.log.info("Finished executing delete file sftp operator")
        return True


class BulkDeleteFileSFTPOperator(BaseOperator):
    template_fields = ("source_path",)

    @apply_defaults
    def __init__(self, sftp_conn_id, source_path, workers=4, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sftp_conn_id = sftp_conn_id
        self.source_path = source_path
        self.workers = workers

    def delete_file(self, file_path):
        for i in range(0, 5):
            try:
                print(f"Deleting {file_path}")
                sftp_hook = SFTPHook(ftp_conn_id=self.sftp_conn_id)
                sftp_hook.no_host_key_check = True
                sftp_hook.delete_file(file_path)
                sftp_hook.close_conn()
                break
            except Exception:
                i += 1
                time.sleep(2 ** i)
                if i >= 4:
                    raise

        return True

    def execute(self, context):
        self.log.info("Going to start bulk delete file sftp operator")
        sftp_hook = SFTPHook(ftp_conn_id=self.sftp_conn_id)
        sftp_hook.no_host_key_check = True

        source_files_list = sftp_hook.list_directory(self.source_path)
        source_files_list = [
            os.path.join(self.source_path, x) for x in source_files_list
        ]

        self.log.info(
            f"Going to delete {len(source_files_list)} with {self.workers} workers"
        )
        with Pool(self.workers) as pool:
            pool.map(self.delete_file, source_files_list)

        self.log.info("Finished executing bulk delete file sftp operator")
        return True


class SftpMultipleAuthenticationHook(BaseHook):
    """
    Since some acquirers does not use openssh standard specification, paramiko
    and pysftp cannot handle with all of them. So this is an alternate version
    of sftp hook that uses bash, ssh and sshpass to run sftp commands
    """

    def __init__(self, sftp_conn_id):
        self.sftp_conn_id = sftp_conn_id
        self.conn = self.get_connection(self.sftp_conn_id)

    def execute_command(self, eof):
        with NamedTemporaryFile("w") as temp_file:
            temp_file.write(self.conn.extra_dejson.get("private_key"))
            temp_file.flush()
            cmd = f"sshpass -p {self.conn.password} sftp -o StrictHostKeyChecking=no -P {self.conn.port or 22} -i {temp_file.name} {self.conn.login}@{self.conn.host} << %EOF%\n{eof}\n%EOF%"
            self.log.info(f"Going to run: {cmd}")
            subprocess.call(cmd, shell=True)


class SftpUtils(AirflowPlugin):
    name = "sftp_utils"
    operators = [
        BulkS3ToSFTPOperator,
        BulkSFTPToS3Operator,
        RenameSFTPOperator,
        BulkRenameSFTPOperator,
        DeleteFileSFTPOperator,
        BulkDeleteFileSFTPOperator,
    ]
    hooks = [SftpMultipleAuthenticationHook]
    executors = []
    macros = []
    admin_views = []
