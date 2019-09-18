from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries
import create_tables

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 table="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.table = table
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        #self.log.info('LoadFactOperator not implemented yet')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Creating Redshift Fact Table")
        formatted_sql = create_tables.CREATE_FACTSongplay_TABLE_SQL
        redshift.run(formatted_sql)
        self.log.info("Clearing data from Fact Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))
        self.log.info("Loading data into Redshift Fact Table")
        formatted_sql = SqlQueries.songplay_table_insert
        redshift.run(formatted_sql)
        self.log.info("Loading data into Fact table {} have been completed!".format(self.table))
