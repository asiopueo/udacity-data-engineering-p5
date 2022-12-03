from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator

from datetime import datetime

dag = DAG("drop_tables", start_date=datetime.now(), schedule_interval="@once")

drop_tables = PostgresOperator(
    task_id="drop_tables",
    dag=dag,
    postgres_conn_id="redshift",
    sql="""
<<<<<<< HEAD
        DROP TABLE IF EXISTS public.artists;
=======
        DROP TABLE IF EXISTS DROP TABLE public.artists;
>>>>>>> 60ba0096f1703c9d6826a6934bf119faa57aabf4
        DROP TABLE IF EXISTS public.songplays;
        DROP TABLE IF EXISTS public.songs;
        DROP TABLE IF EXISTS public.staging_events;
        DROP TABLE IF EXISTS public.staging_songs;
        DROP TABLE IF EXISTS public.time;
        DROP TABLE IF EXISTS public.users;
    """
)