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
                 append_data=False,
                 insert_query="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.append_data = append_data
        self.table = table
        self.insert_query = insert_query

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.append_data:
            sql_statement = 'INSERT INTO %s %s' % (self.table_name, self.sql_statement)
            redshift.run(sql_statement)

            redshift.run(trunc_query.format(self.table))
            

        sql_statement = 'DELETE FROM %s' % self.table_name
        redshift.run(sql_statement)

        sql_statement = 'INSERT INTO %s %s' % (self.table_name, self.sql_statement)
        redshift.run(sql_statement)
        
        self.log.info(f"Inserted data into {self.table}")



# With dimension and fact operators, you can utilize the provided SQL helper class to
# run data transformations. Most of the logic is within the SQL transformations and the 
# operator is expected to take as input a 5QL statement and target database on which to
# run the query against. You can also define a target table that will contain the results 
# of the transformation. 

# Fact tables are usually so massive that they should only allow append type functionality. 