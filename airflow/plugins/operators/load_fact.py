from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 table="",
                 append_only="",
                 load_fact_sql="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.append_only = append_only
        self.load_fact_sql = load_fact_sql

    def execute(self, context):
        self.log.info('Starting to load into fact table')
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        # Append_only set to True by default for fact table
        
        if self.append_only:
            load_fact_sql = (f"INSERT INTO {self.table}(cicid,city,code){self.load_fact_sql}")
            redshift.run(load_fact_sql)
        
        self.log.info('Loaded data into fact table')
