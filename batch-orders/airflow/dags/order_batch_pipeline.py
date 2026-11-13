"""주문 마트 생성 후 활동 상세 집계를 실행한다."""

import os
from datetime import datetime, timedelta

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import DAG
from docker.types import Mount

with DAG(
    dag_id="order_batch_pipeline",
    start_date=datetime(2026, 10, 28),
    schedule=None,
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(seconds=30)},
    tags=["orders", "batch"],
) as dag:
    spark_options = dict(
        image=os.environ.get("SPARK_IMAGE", "quay.io/jupyter/pyspark-notebook:latest"),
        docker_url="unix://var/run/docker.sock",
        mounts=[
            Mount(source=os.environ["BATCH_PROJECT_DIR"], target="/home/jovyan/work", type="bind")
        ],
        working_dir="/home/jovyan/work",
        mount_tmp_dir=False,
        auto_remove="success",
    )
    order_mart = DockerOperator(
        task_id="build_order_mart",
        command="spark-submit jobs/build_order_mart.py",
        **spark_options,
    )
    activity_detail = DockerOperator(
        task_id="build_activity_detail",
        command="spark-submit jobs/build_activity_detail.py",
        **spark_options,
    )
    order_mart >> activity_detail
