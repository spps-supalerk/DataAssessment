import pandas as pd
import numpy as np
import pyodbc
import warnings
import glob
from tqdm import tqdm

# connect information 
driver = 'your_driver_name'
server = 'your_sever_name'
database = 'your_database_name'
uid = 'your_user_id'
pwd = 'your_password'

# sql select information_schema
table = '''SELECT TABLE_NAME,
            COLUMN_NAME,
            DATA_TYPE,
            FROM INFORMATION_SCHEMA.COLUMNS;'''

# output directory example : 'C:\\Users\\abc\\'
dir_output = 'your_dir_output'

conn = pyodbc.connect(f'''DRIVER={'{'+driver+'}'};
                          SERVER={server};
                          DATABASE={database};
                          UID={uid};
                          PWD={pwd}''')
                          
# create df which contains database information_schema
table_name = pd.read_sql(table , conn)

# get system name from TABLE_NAME
table_name['system'] = table_name['TABLE_NAME'].apply(lambda x:x.split('_')[0])


# read file and keep finished file into list : drop_finished_table 
drop_finished_table = []
all_file_path = glob.glob(f'{dir_output}*.csv')
for i in all_file_path:
    i = i.split('\\')[-1].split('.')[0]
    drop_finished_table.append(i)

# keep transactions which are not in list : drop_finished_table 
table_name = table_name[~(table_name['TABLE_NAME'].isin(drop_finished_table))]


# check numerical data tpye for processing numerical column
numeric_type = ['bigint', 
                'int',
                'float',
                'decimal', 
                'money', 
                'tinyint', 
                'numeric', 
                'smallint', 
                'bit']

# loop through unique system in df
for system in tqdm(table_name['system'].unique()):

    # filter system
    table_in_system = table_name[table_name['system']==system]
    
    # create df template
    table_result = pd.DataFrame( columns = ['system_name' ,
                                            'table_name' , 
                                            'column_name' , 
                                            'dtype',
                                            'nulls' , 
                                            'not_nulls' ,
                                            'max',
                                            'min',
                                            'nunique',
                                            'average'])

    print(f'<<<<< Start System : {system} >>>>>')

    # loop through unique table in filtered system
    for table in table_in_system['TABLE_NAME'].unique(): 

        # filter table
        col_in_system = table_in_system[table_in_system['TABLE_NAME']==table]
        
        # loop through column in filtered system and table
        for col in col_in_system['COLUMN_NAME']:

            # get data type
            data_type = col_in_system[col_in_system['COLUMN_NAME']==col]['DATA_TYPE'].values[0]

            # process if data type in list : numeric_type
            if data_type in numeric_type:
                sql_result_code = f'''select
                        '{system}' as system_name,
                        '{table}' as table_name,
                        '{col}' as column_name,
                        '{data_type}' as dtype,
                        sum(case when [{col}] is null then 1 else 0 end)  as nulls,
                        count([{col}]) as not_nulls ,
                        max(CAST([{col}] AS BIGINT)) as max,
                        min(CAST([{col}] AS BIGINT)) as min,
                        count (distinct [{col}]) as nunique,
                        AVG(CAST([{col}] AS DECIMAL(38,0))) as average
                        from [DATACAFE].[dbo].[{table}]'''
                result = pd.read_sql(sql_result_code , conn)

                # concat result with df : table_result
                table_result = pd.concat([table_result , result])
                print(f'Finished | system : {system}, table : {table}, col : {col}, time : {pd.datetime.now()}')

            # process if data type is image
            elif data_type == 'image':
                sql_result_code = f'''select
                        '{system}' as system_name,
                        '{table}' as table_name,
                        '{col}' as column_name,
                        '{data_type}' as dtype,
                        sum(case when [{col}] is null then 1 else 0 end)  as nulls,
                        sum(case when [{col}] is null then 0 else 1 end) as not_nulls ,
                        'NA' as max,
                        'NA' as min,
                        'NA' as nunique,
                        'NA' as average
                        from [DATACAFE].[dbo].[{system}]'''
                result = pd.read_sql(sql_result_code , conn)

                # concat result with df : table_result
                table_result = pd.concat([table_result , result])
                print(f'Finished | system : {system}, table : {table}, col : {col}, time : {pd.datetime.now()}')
                
            else:
                # process if data type is str or others   
                sql_result_code = f'''select
                        '{system}' as system_name,
                        '{table}' as table_name,
                        '{col}' as column_name,
                        '{data_type}' as dtype,
                        sum(case when [{col}] is null then 1 else 0 end)  as nulls,
                        sum(case when [{col}] is null then 0 else 1 end) as not_nulls ,
                        'NA' as max,
                        'NA' as min,
                        count (distinct CAST([{col}] AS VARCHAR(MAX)))as nunique,
                        'NA' as average
                        from [DATACAFE].[dbo].[{table}]'''
                result = pd.read_sql(sql_result_code , conn)

                # concat result with df : table_result
                table_result = pd.concat([table_result , result])
                print(f'Finished | system : {system}, table : {table}, col : {col}, time : {pd.datetime.now()}')

        # select columns     
        table_result = table_result[['system_name','table_name','column_name', 
                                    'dtype', 'max', 'min','average' ,'not_nulls', 
                                    'nulls','nunique']]

        # write result of each table in dir_output/table_name
        table_result.to_csv(f'{dir_output}\\{table}.csv')

        print(f'>>>>> Finished TABLE : {table} {pd.datetime.now()} <<<<<')
