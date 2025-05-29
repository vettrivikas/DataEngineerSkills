from airflow.datasets import Dataset

level_1_job_1_dataset = Dataset("ds://level_1_job_1")
level_1_job_2_dataset = Dataset("ds://level_1_job_2")
level_1_job_3_dataset = Dataset("ds://level_1_job_3")
level_1_job_4_dataset = Dataset("ds://level_1_job_4")

level_3_jobs_2_dataset = Dataset("ds://level_3_jobs_2")  # intermediate dataset
