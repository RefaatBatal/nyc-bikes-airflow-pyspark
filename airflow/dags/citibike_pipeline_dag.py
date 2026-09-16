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
        import time
        from datetime import datetime, timezone
        from elasticsearch import Elasticsearch
        from citibike_pipeline import main

        es = Elasticsearch("http://elasticsearch:9200")
        started_at = datetime.now(timezone.utc).isoformat()
        start_time = time.time()
        status = "success"
        error_message = None
        metrics = {}
        try:
            metrics = main()
        except Exception as e:
            status = "failed"
            error_message = str(e)
            raise
        finally:
            doc = {
                "dag_id": "citibike_top_stations",
                "started_at": started_at,
                "duration_seconds": round(time.time() - start_time, 2),
                "status": status,
                "error_message": error_message,
                **metrics,
            }
            es.index(index="pipeline_metrics", document=doc)

    run_top_stations_analysis()

    
citibike_top_stations()