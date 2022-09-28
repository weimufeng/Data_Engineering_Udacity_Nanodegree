from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 tables=[],
                 dq_checks = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables
        self.dq_checks = dq_checks

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)

        self.log.info("Validating Data Quality.")
        
        self.log.info("Null value checking")
        for check in dq_checks:
            sql = check.get('check_sql')
            exp_result = check.get('expected_result')
 
            records = redshift.get_records(sql)[0]
    
            error_count = 0
 
            if exp_result != records[0]:
                error_count += 1
                failing_tests.append(sql)
 
            if error_count > 0:
                self.log.info('Null value checking failed')
                self.log.info(failing_tests)
                raise ValueError('Data quality check failed!')
            
            if error_count == 0:
                self.log.info('Null value checking passed!')
        
        for table in self.tables:
            self.log.info(f"   Validating Data Quality for {table} table.")
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
            
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {table} returned no results")
            num_records = records[0][0]
            
            if num_records < 1:
                raise ValueError(f"Data quality check failed: {table} contained 0 rows")
                
            self.log.info(f"Data quality on table {table} check passed with {records[0][0]} records")