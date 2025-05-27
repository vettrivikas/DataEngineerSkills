from __future__ import annotations

import pendulum

from airflow.datasets import Dataset
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

# [START dataset_def]
dag1_dataset = Dataset("s3://dag1/output_1.txt", extra={"hi": "bye"})
# [END dataset_def]
dag2_dataset = Dataset("s3://dag2/output_1.txt", extra={"hi": "bye"})

with DAG(
    dag_id="dataset_produces_1",
    catchup=False,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule="@daily",
    tags=["produces", "dataset-scheduled"],
) as dag1:
    # [START task_outlet]
    BashOperator(outlets=[dag1_dataset], task_id="producing_task_1", bash_command="sleep 5")
    # [END task_outlet]

with DAG(
    dag_id="dataset_produces_2",
    catchup=False,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule=None,
    tags=["produces", "dataset-scheduled"],
) as dag2:
    BashOperator(outlets=[dag2_dataset], task_id="producing_task_2", bash_command="sleep 5")

# [START dag_dep]
with DAG(
    dag_id="dataset_consumes_1",
    catchup=False,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule=[dag1_dataset],
    tags=["consumes", "dataset-scheduled"],
) as dag3:
    # [END dag_dep]
    BashOperator(
        outlets=[Dataset("s3://consuming_1_task/dataset_other.txt")],
        task_id="consuming_1",
        bash_command="sleep 5",
    )

with DAG(
    dag_id="dataset_consumes_1_and_2",
    catchup=False,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule=[dag1_dataset, dag2_dataset],
    tags=["consumes", "dataset-scheduled"],
) as dag4:
    BashOperator(
        outlets=[Dataset("s3://consuming_2_task/dataset_other_unknown.txt")],
        task_id="consuming_2",
        bash_command="sleep 5",
    )

with DAG(
    dag_id="dataset_consumes_1_never_scheduled",
    catchup=False,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule=[
        dag1_dataset,
        Dataset("s3://this-dataset-doesnt-get-triggered"),
    ],
    tags=["consumes", "dataset-scheduled"],
) as dag5:
    BashOperator(
        outlets=[Dataset("s3://consuming_2_task/dataset_other_unknown.txt")],
        task_id="consuming_3",
        bash_command="sleep 5",
    )

with DAG(
    dag_id="dataset_consumes_unknown_never_scheduled",
    catchup=False,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule=[
        Dataset("s3://unrelated/dataset3.txt"),
        Dataset("s3://unrelated/dataset_other_unknown.txt"),
    ],
    tags=["dataset-scheduled"],
) as dag6:
    BashOperator(
        task_id="unrelated_task",
        outlets=[Dataset("s3://unrelated_task/dataset_other_unknown.txt")],
        bash_command="sleep 5",
    )
