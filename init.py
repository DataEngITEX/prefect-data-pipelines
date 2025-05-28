# flow.py
from prefect import flow, task
import subprocess
import os

@task
def run_spark_job():
    try:
        result = subprocess.run(
            #[r"C:\Spark\spark-3.5.5-bin-hadoop3\bin\spark-submit.cmd", "spark.py"],
            ["spark-submit", "spark.py"],
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
    run_spark_job()

if __name__ == "__main__":
    spark_flow()
