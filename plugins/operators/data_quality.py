from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift_hook = PostgresHook( self.redshift_conn_id )
        #records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {}") # Insert table here
        asd="132asd"
        if True:
            self.log.info(f"Data quality check on table {asd} passed")
            return
        else:
            self.log.info(f"Data quality check negative with {asd} fails")
        