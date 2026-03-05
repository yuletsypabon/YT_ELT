import logging
from airflow.operators.bash  import BashOperator  

logger = logging.getLogger(__name__)

SODA_PATH = "/opt/airflow/include/soda"
SODA_BIN = "/home/airflow/.local/bin/soda"
DATASOURCE = "my_datasource"  


def yt_elt_data_quality_check(schema):
    try:
        task = BashOperator(
            task_id=f"soda_test_{schema}",
            bash_command = f"{SODA_BIN} scan -d {DATASOURCE} -c {SODA_PATH}/configuration.yml -v SCHEMA={schema} {SODA_PATH}/checks.yml",
    )
        return task
    except Exception as e:
        logger.error(f"Error creating Soda scan task for schema {schema}")
        raise e 