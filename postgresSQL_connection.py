from configparser import ConfigParser
import pandas as pd
import numpy as np
import psycopg2
import psycopg2.extras as extras

from config import config as configFunction
from sqlalchemy import create_engine


def config(archivo='database.ini',seccion=''):
    # Create parser and read the archive
    parser = ConfigParser()
    parser.read(archivo)
 
    # create a conection to the database
    db = {}
    if parser.has_section(seccion):
        params = parser.items(seccion)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Secccion {0} no encontrada en el archivo {1}'.format(seccion, archivo))

    return db


 params_dic = config(archivo='database.ini', seccion='pgsql_database')

 def execute_values(conn, df, table):
    print("execute_values() start")
    # Create a list of tupples from the dataframe values
    tuples = [tuple(x) for x in df.to_numpy()]
    # Comma-separated dataframe columns
    cols = ','.join(list('"'+df.columns+'"'))
    print(cols)
    # SQL quert to execute
    query  = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("execute_values() \n\n finished")
    cursor.close()


df_7S = pd.Dataframe()     ## Dataframe-table for exporting ##
##### BATCH AGREGATION (LOTES) #####
for i in range(34):
    #i = i+7
    lote = i*10000
    conn = None
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params_dic)
        print('Connecting successfull PostgreSQL database...')
    except (Exception, psycopg2.DatabaseError) as error:
            print(error)
    
    alchemyEngine = create_engine('postgresql://'+str(params_dic['user'])+':'+str(params_dic['password'])+'@'+str(params_dic['host'])+':'+str(params_dic['port'])+'/'+str(params_dic['database']));
    postgreSQLConnection    = alchemyEngine.connect();
    postgreSQLTable         = 'table_name';
    
    if i == 0: sch = 'replace'
    else: sch = 'append'
        
    try:
        df_7S[lote:lote+10000].to_sql(postgreSQLTable, 
                            con = postgreSQLConnection, 
                            schema = 'schema_name',
                            if_exists = sch, 
                            index=False);
    except ValueError as vx: print(vx)
    except Exception as ex: print(ex)

    else: print("PostgreSQL Table %s has been created successfully."%postgreSQLTable);
    finally:
        print("Lote "+ str(lote) + " Exitoso")
        print(sch)
        postgreSQLConnection.close();
    print("-----------")


conn.close()   #Close connection#

