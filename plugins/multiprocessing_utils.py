from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
from airflow.operators.python_operator import PythonOperator
import multiprocessing


class PythonThreadingOperator(PythonOperator):
    @apply_defaults
    def __init__(self, workers=4, dummy=True, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.workers = workers
        if dummy:
            self.Pool = multiprocessing.dummy.Pool
        else:
            self.Pool = multiprocessing.Pool

    def execute(self, context):
        self.log.info("Starting executing Python Threading Operator")

        with self.Pool(processes=self.workers) as pool:
            pool.map(self.python_callable, self.op_args)


class MultiprocessingUtils(AirflowPlugin):
    name = "multiprocessing_utils"
    operators = [PythonThreadingOperator]
    hooks = []
    executors = []
    macros = []
    admin_views = []
