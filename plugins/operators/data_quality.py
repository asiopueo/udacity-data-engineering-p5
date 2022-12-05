from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 quality_checks,
                 redshift_conn_id = "",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.quality_checks = quality_checks

    def execute(self, context):
        redshift_hook = PostgresHook( self.redshift_conn_id )
        #records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {}") # Insert table here
        self.log.info("Starting data quality checks.")
        
        for check in self.quality_checks:
            records = redshift_hook.get_records(check['sql'])
            
            if check['type'] == 'eq' and records[0][0] != check['comparison']:
                raise ValueError(f"Data quality check failed! The query {check['sql']} expected {check['comparison']}, but received {records[0][0]}")
            elif check['type'] == 'gt' and records[0][0] <= check['comparison']:
                raise ValueError(f"Data quality check failed! The query {check['sql']} expected a value greater the  {check['comparison']}, but received {records[0][0]}")
                
        self.log.info(f"All data quality checks were successful.")