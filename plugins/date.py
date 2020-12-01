from jinja2 import contextfunction
from airflow.plugins_manager import AirflowPlugin
from datetime import date, datetime


def _get_date_property(context, config_date_name, execution_date_name):
    try:
        return context["dag_run"].conf[config_date_name]
    except (AttributeError, KeyError):
        pass

    try:
        return context["params"][config_date_name]
    except KeyError:
        pass

    return context[execution_date_name].isoformat()


@contextfunction
def start(context):
    return _get_date_property(
        context, config_date_name="start_date", execution_date_name="execution_date"
    )


@contextfunction
def end(context):
    return _get_date_property(
        context, config_date_name="end_date", execution_date_name="next_execution_date"
    )


def truncate(interval, datevalue):
    if interval == "monthly":
        return date(datevalue.year, datevalue.month, 1)
    elif interval == "daily":
        return date(datevalue.year, datevalue.month, datevalue.day)
    else:
        return datevalue


def fmtnow():
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")


def format(time_value):
    return time_value.strftime("%Y-%m-%dT%H:%M:%S")


def format_nodash(time_value):
    return time_value.strftime("%Y%m%dT%H%M%S")


class DatesUtilPlugin(AirflowPlugin):
    name = "date"
    operators = []
    hooks = []
    executors = []
    macros = [start, end, truncate, fmtnow, format, format_nodash]
    admin_views = []
