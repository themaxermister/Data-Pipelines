from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    insert_sql = """
        INSERT INTO {}
        {}
        """
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 trunc_bool=False,
                 insert_query="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.trunc_bool = trunc_bool
        self.table = table
        self.insert_query = insert_query

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.trunc_bool:
            trunc_query = """"
                TRUNCATE TABLE {};
            """
            redshift.run(trunc_query.format(self.table))
            self.log.info(f"TRUNCATED {self.table}")

        self.log.info(f"Inserting data into {self.table}")
        formatted_sql = LoadFactOperator.insert_sql.format(
            self.table,
            self.insert_query
        )
        redshift.run(formatted_sql)
        self.log.info(f"{self.table} LOADED")



# With dimension and fact operators, you can utilize the provided SQL helper class to
# run data transformations. Most of the logic is within the SQL transformations and the 
# operator is expected to take as input a 5QL statement and target database on which to
# run the query against. You can also define a target table that will contain the results 
# of the transformation. 

# Fact tables are usually so massive that they should only allow append type functionality. 