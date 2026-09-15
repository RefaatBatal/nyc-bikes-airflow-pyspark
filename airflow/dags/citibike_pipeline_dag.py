from airflow.sdk import dag, task
from datetime import datetime, timedelta

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def send_failure_alert(context):
    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    exception = context.get("exception")
    print(f"ALERT: Task '{task_id}' in DAG '{dag_id}' failed after all retries. Reason: {exception}")


@dag(
    dag_id="citibike_top_stations",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["pyspark", "citibike"],
)

def citibike_top_stations():

    @task(on_failure_callback=send_failure_alert)
    def run_top_stations_analysis():
        from citibike_pipeline import main
        main()

    run_top_stations_analysis()

    
citibike_top_stations()