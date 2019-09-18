from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries
import create_tables

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    #create_table_sql =  "create_tables.CREATE_DIM_{}_TABLE_SQL"
    #DDL =  "{}"
    #DML = "{}"
    #print(DDL)
    #print(DML)
    
    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 table="",
                 DDL="",
                 DML="",
                 LS="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.DDL = DDL
        self.DML = DML
        self.LS = LS

    def execute(self, context):
        #self.log.info('LoadDimensionOperator not implemented yet')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Creating Redshift {} Dimension Table".format(self.table))
        #formatted_sql = self.DDL
        #formatted_sql = "create_tables.CREATE_DIM_{}_TABLE_SQL".format(self.table)
        #formatted_sql = create_tables.CREATE_DIM_users_TABLE_SQL
        #formatted_sql = LoadDimensionOperator.create_table_sql.format(self.table)
        #formatted_sql = LoadDimensionOperator.create_table_sql.format(self.table)
        #print(formatted_sql)
        redshift.run(self.DDL)
        if self.LS == 'DL':
            self.log.info("Delete-Load mode is initiated")
            self.log.info("Clearing data from Redshift {} table".format(self.table))
            redshift.run("DELETE FROM {}".format(self.table))
            self.log.info("Loading data into Redshift {} Table".format(self.table))
            redshift.run(self.DML) 
        else:
            self.log.info("Append Only mode is initiated")
            self.log.info("Appending data to Redshift {} table".format(self.table))
            redshift.run(self.DML)
       
            
       
        
