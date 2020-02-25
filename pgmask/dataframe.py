import pandas as pd
from sqlalchemy import create_engine

class DataframeLayerOverDb: #TODO: Docstrings and Type annotations
    
    def __init__(self, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME):
        
        super().__init__()
        self.DB_USER = DB_USER
        self.DB_PASSWORD = DB_PASSWORD
        self.DB_HOST = DB_HOST
        self.DB_PORT = DB_PORT
        self.DB_NAME = DB_NAME

        
    def read_by_sql(self, sql_query):
        
        try:
        
            dataframe = pd.DataFrame()

            with psycopg2.connect("host={} port={} dbname={} user={} password={}".\
                                  format(self.DB_HOST, self.DB_PORT, self.DB_NAME, 
                                        self.DB_USER, self.DB_PASSWORD)) as connection:

                dataframe = pd.read_sql(sql_query, connection, index_col=None,coerce_float=True,
                                                 params=None, parse_dates=None, columns=None, chunksize=None)

                
            return dataframe
        
        except Exception as err:
            
            print(err)
            
            table_not_found = bool(("relation" and "does not exist") in str(err))
            
            if (table_not_found): raise Exception('Table not found')


    def read_entries_list(self, table_name, column_name, entries_list):

        sql_query = "SELECT * FROM " + table_name +" WHERE "+ column_name + "  IN ("

        for entry in entries_list:

            sql_query = sql_query + "'" + entry + "',"

        sql_query = sql_query[:len(sql_query) - 1] + ")"
        
        return self.read_by_sql(sql_query)

    
    def read_all_latest_entries(self, table_name):
        
        sql_query = 'SELECT * FROM ' + table_name
        
        return self.read_by_sql(sql_query)
    
    
    def read_the_n_latest_entries(self, table_name, field_key, number_entries):

        query = 'SELECT * FROM ' + table_name + ' ORDER BY ' + field_key + ' DESC LIMIT ' + str(number_entries)

        return (self.read_by_sql(query)).sort_values(by = field_key, 
        axis=0, ascending=True, inplace=False, kind='quicksort', na_position='last', ignore_index=True)
    
    
    def save_df_into_db(self, table_name, dataframe_in, if_exists='replace', **kwargs):
        
        column_for_index = kwargs.get('column_for_index')

        engine = create_engine('postgresql+psycopg2://{}:{}@{}:{}/{}'.\
                               format(self.DB_USER, self.DB_PASSWORD, self.DB_HOST, 
                                      self.DB_PORT, self.DB_NAME))

        try:

            dataframe_in.to_sql(table_name, con=engine, schema=None, if_exists=if_exists, index = False,
                                index_label=column_for_index, chunksize=None, dtype=None, method=None)

        except Exception as err: pass #TODO: tratar exceção aqui