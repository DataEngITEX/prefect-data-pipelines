# flow.py
from prefect import flow, task
import subprocess
import os



@task
def set_environment():
    os.environ["PYSPARK_PYTHON"] =r"C:\Users\data.engineer\Documents\Prefect\prefect-venv\Scripts\python"

@task
def run_spark_job():
    try:
        result = subprocess.run(
           # [r"C:\Spark\spark-3.5.5-bin-hadoop3\bin\spark-submit.cmd", "spark.py"],
            ["spark-submit", "pipelines/cbn_report_pipeline/cbn_report.py"],
            shell=True,
            capture_output=True,
            text=True,
            check=True
        )
        print("Spark Output:\n", result.stdout)
    except subprocess.CalledProcessError as e:
        print("Spark Error:\n", e.stderr)
        raise
@flow
def spark_flow():
    set_environment()
    run_spark_job()#calls the spark submit

if __name__ == "__main__":
    spark_flow()
