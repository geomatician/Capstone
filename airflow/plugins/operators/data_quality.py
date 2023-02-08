from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials="",
                 tables="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.tables=tables

    def execute(self, context):
        self.log.info("Starting data quality checks")
        redshift_hook= PostgresHook(postgres_conn_id=self.redshift_conn_id)

        """
        Check the count of rows in all tables, one by one
        """
        if len(self.tables) > 1:
            for table in self.tables:
                self.log.info(f"SELECT COUNT(*) FROM {table}")
                records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")

                if len(records) < 1 or len(records[0]) < 1:
                    raise ValueError(f"Data quality check failed. {table} returned no results")
                num_records = records[0][0]
                if num_records < 1:
                    raise ValueError(f"Data quality check failed. {table} contained 0 rows")
                self.log.info(f"Data quality on table {table} check passed with {records[0][0]} records")
        else:   
            """
            Check uniqueness of fact table's column (i.e. cicid key)  
            """
            self.log.info(f"Evaluating uniqueness of cicid column of table {self.tables[0]}")
            
            
            records = redshift_hook.get_records(f"SELECT NOT EXISTS (SELECT cicid, COUNT(*) \
                                                    from public.fact \
                                                    GROUP BY cicid \
                                                    HAVING COUNT(*) > 1)")
            if not records[0][0]:
                raise ValueError(f"Fact table {self.tables[0]}'s cicid column is not unique.")
            else:
                self.log.info(f"Fact table {self.tables[0]}'s cicid column is unique.")
                
        
           