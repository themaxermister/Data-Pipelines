from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    insert_sql = """
        INSERT INTO {}
        {}
        """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 insert_query="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.insert_query = insert_query

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info(f"Inserting data into {self.table}")
        formatted_sql = LoadDimensionOperator.insert_sql.format(
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

# Dimension loads are often done with the truncate-insert pattern where the target table is 
# emptied before the load. Thus, you could also have a parameter that allows switching between
# insert modes when loading dimensions.