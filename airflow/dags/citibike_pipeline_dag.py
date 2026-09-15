from airflow.sdk import dag, task
from datetime import datetime


@dag(
    dag_id = "citibike_top_stations",
    schedule = None,
    start_date=datetime(2026,1,1),
    catchup=False,
    tags=["pyspark", "citibike"],
)

def citibike_top_stations():

    @task
    def run_top_stations_analysis():
        from citibike_pipeline import main
        main()
    run_top_stations_analysis()
citibike_top_stations()