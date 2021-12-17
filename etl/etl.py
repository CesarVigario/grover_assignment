from numpy import dtype
import psycopg2 
from psycopg2.errors import OperationalError
import pandas as pd
import os
import sys
from sqlalchemy import create_engine
from datetime import date
from sqlalchemy.types import INTEGER, VARCHAR, DATE
from sqlalchemy.exc import IntegrityError

class Etl():

    def __init__(self):
        """Initiate the ETL class."""

        database_url = os.environ.get("DATABASE_URL_SECRET")
        try:
            self.pg_conn = psycopg2.connect(database_url)
        except:
            print("UNABLE TO CONNECT TO DATABASE!")
            sys.exit(1)

    def executeScriptsFromFile(self, filename):
        """Execute SQL code directly from sql file."""
        
        fd = open(filename, 'r')
        sqlFile = fd.read()
        fd.close()

        sqlCommands = sqlFile.split(';') # all SQL commands (split on ';')

        # Execute every command from the input file
        i = 0
        for command in sqlCommands:
            if i != len(sqlCommands)-1:
                print(command)
                # This will skip and report errors
                # For example, if the tables do not yet exist, this will skip over
                # the DROP TABLE commands
                try:
                    self.pg_conn.commit()
                    cur = self.pg_conn.cursor()
                    cur.execute(command)
                    self.pg_conn.commit()
                    cur.close()
                except OperationalError as msg:
                    print("Command skipped: ", msg)
                except psycopg2.errors.DuplicateTable as e:
                    print(e)
            i += 1

    def load_csv_to_postgres(self, table_name):
        """Loads csv to postgres tables."""

        cur = self.pg_conn.cursor()
        csv_file_name = f'../sample_files/exercise1/{table_name}.csv'
        sql = f"COPY grover.{table_name} FROM STDIN DELIMITER ',' CSV HEADER"
        try:
            self.pg_conn.commit()
            cur.copy_expert(sql, open(csv_file_name, "r"))
            self.pg_conn.commit()
            cur.close()
        except psycopg2.errors.DatetimeFieldOverflow:
            df = pd.read_csv(f"../sample_files/exercise1/{table_name}.csv")
            # make sure creation_date is interpreted as date instead of string
            df['creation_date'] = pd.to_datetime(df['creation_date'], infer_datetime_format=True).dt.date
            df.rename(columns={"status_id": "status_code"}, inplace=True)
            self.load_dataframe_to_postgres(df, table_name, schema_name='grover')
        except psycopg2.errors.UniqueViolation:
            print("Primary key already exists. Rows not appended.")

    def load_dataframe_to_postgres(self, df, table_name, schema_name):
        """Load dataframe to postgres table."""

        database_url = os.environ.get("DATABASE_URL_SECRET2")
        con = create_engine(database_url)
        try:
            df.to_sql(name=table_name, con = con, schema = schema_name, if_exists='append', index = False)
        except IntegrityError:
            print("Primary key already exists. Rows not appended.")

    def get_data_from_postgres(self, table_name, schema_name):
        """Extract data from postgres database."""

        self.pg_conn.commit()
        select_query = f"SELECT * FROM {schema_name}.{table_name};"
        df_query = pd.read_sql_query(sql=select_query, con=self.pg_conn)

        return df_query

    def main(self):
        """Where all the things happen."""

        # create source tables from sql file
        self.executeScriptsFromFile("../sql/exercise1/ingestion_tables_ddl.sql")

        # load csv to table
        tables = ['orders','country','order_status', 'category']
        for table_name in tables:
            self.load_csv_to_postgres(table_name)

        # extract tables from posgres
        df_orders = self.get_data_from_postgres(table_name='orders', schema_name='grover')
        df_country = self.get_data_from_postgres(table_name='country', schema_name='grover')
        df_order_status = self.get_data_from_postgres(table_name='order_status', schema_name='grover')
        df_category = self.get_data_from_postgres(table_name='category', schema_name='grover')

        ############## build the dimensions ################
        # create dim_date
        self.executeScriptsFromFile("../sql/exercise1/dim_date_ddl.sql")
        # populate dim_date
        self.executeScriptsFromFile("../sql/exercise1/populate_dim_date.sql")

        # create dim_month
        self.executeScriptsFromFile("../sql/exercise1/dim_month_ddl.sql")
        # populate dim_month
        self.executeScriptsFromFile("../sql/exercise1/populate_dim_month.sql")

        # create dim_order_status
        self.executeScriptsFromFile("../sql/exercise1/dim_order_status_ddl.sql")
        # populate dim_order_status - we could add relevant information regarding the status
        # more details like creation date of the status, description, etc
        dim_order_status = df_order_status
        dim_order_status['effectivedate'] = date.today()
        dim_order_status.rename(columns={'status_code': 'status_id', 'status': 'status_name'}, inplace=True)
        self.load_dataframe_to_postgres(df=dim_order_status, table_name='dim_order_status', schema_name='grover_dwh')

        # create dim_country
        self.executeScriptsFromFile("../sql/exercise1/dim_country_ddl.sql")
        # populate dim_country
        # we can have more columns like continent, population, capital, fk_city, etc
        dim_country = df_country
        dim_country['effectivedate'] = date.today()
        dim_country.rename(columns={'id': 'country_id'}, inplace=True)
        self.load_dataframe_to_postgres(df=dim_country, table_name='dim_country', schema_name='grover_dwh')

        # create dim_category
        self.executeScriptsFromFile("../sql/exercise1/dim_category_ddl.sql")
        # populate dim_category
        # we can have more columns like sub_category, description, active (true/false)
        dim_category = df_category
        dim_category['effectivedate'] = date.today()
        dim_category.rename(columns={'id': 'category_id'}, inplace=True)
        self.load_dataframe_to_postgres(df=dim_category, table_name='dim_category', schema_name='grover_dwh')

        # create dim_orders
        self.executeScriptsFromFile("../sql/exercise1/dim_orders_ddl.sql")
        # populate dim_orders
        # pandas way:
        dim_orders = df_orders.merge(df_order_status, how='inner', on='status_code').drop(columns='status_code')
        dim_orders = dim_orders.merge(df_country, how='left', left_on='country_id', right_on='id').drop(columns=['id','country_id'])
        dim_orders = dim_orders.merge(df_category, how='left', left_on='category_id', right_on='id').drop(columns=['id','category_id'])
        dim_orders.rename(columns={'status': 'order_status'}, inplace=True)
        self.load_dataframe_to_postgres(df=dim_orders, table_name='dim_orders_test', schema_name='grover_dwh')
        # sql way:
        self.executeScriptsFromFile("../sql/exercise1/populate_dim_orders_ddl.sql")

        ############## build facts ################
        # create fact_monthly_orders
        self.executeScriptsFromFile("../sql/exercise1/fact_monthly_orders_ddl.sql")
        # populate fact_monthly_orders
        # pandas way
        dim_orders = job.get_data_from_postgres(table_name='dim_orders', schema_name='grover_dwh')
        dim_month = job.get_data_from_postgres(table_name='dim_month', schema_name='grover_dwh')
        dim_order_status = job.get_data_from_postgres(table_name='dim_order_status', schema_name='grover_dwh')
        dim_country = job.get_data_from_postgres(table_name='dim_country', schema_name='grover_dwh')
        dim_category = job.get_data_from_postgres(table_name='dim_category', schema_name='grover_dwh')

        # to join dim_orders with dim_month I need to convert the creation_date column 
        dim_orders['created_month_fk'] = pd.to_datetime(dim_orders['creation_date'])
        dim_orders['created_month_fk'] = dim_orders['created_month_fk'].dt.strftime('%Y%m').astype(int)

        fact_month_orders = dim_orders.merge(dim_month['dim_month_id'], how='inner', left_on='created_month_fk', right_on='dim_month_id').drop(columns=['created_month_fk','creation_date','dim_orders_id'])
        fact_month_orders = fact_month_orders.merge(dim_order_status[['dim_order_status_id','status_name']], how='left', left_on='order_status', right_on='status_name').drop(columns=['order_status','status_name'])
        fact_month_orders = fact_month_orders.merge(dim_country[['dim_country_id','country']], how='left', on='country').drop(columns=['country'])
        fact_month_orders = fact_month_orders.merge(dim_category[['dim_category_id','category']], how='left', on='category').drop(columns=['category'])
        fact_month_orders = fact_month_orders.groupby(['dim_month_id','dim_order_status_id','dim_country_id','dim_category_id'], as_index=False).agg({'order_id': 'nunique', 'order_value': 'sum'})
        fact_month_orders.rename(columns={'order_id':'number_of_orders', 'order_value':'total_order_value'}, inplace=True)
        self.load_dataframe_to_postgres(df=fact_month_orders, table_name='fact_monthly_orders', schema_name='grover_dwh')
        # sql way
        self.executeScriptsFromFile("../sql/exercise1/populate_fact_monthly_orders_ddl.sql")


if __name__== "__main__":
    job = Etl()
    job.main()