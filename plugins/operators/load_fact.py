from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 sql_query,
                 table,
                 append_mode,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query
        self.table = table
        self.append_mode = append_mode
        
    def execute(self, context):
        self.log.info(f'Starting load of fact table {self.table}')
        redshift = PostgresHook( self.redshift_conn_id )
        if self.append_mode == True:
            redshift.run( self.sql_query )
        else:
            redshift.run( self.sql_query )
        self.log.info(f'Finished load of table {self.table}')
