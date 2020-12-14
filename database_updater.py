

import psycopg2
import csv
from config import mypass

# fn to update database, to be used with data_exploration.py

def database_update(red_data, white_data, all_data):
    
    # 1) connect to general database, remove current data (updated data should be held in active memory of data exploration)
    try:
        con = psycopg2.connect(user='postgres', password=mypass, host="127.0.0.1", port="5432")
        con.autocommit = True
        cursor = con.cursor()
    
        #print sql connection prperties
        print("Connection Properites: \n")
        print(con.get_dsn_parameters(),"\n")
    
        #print PostgreSQL version
        cursor.execute("SELECT version();")
        db_version = cursor.fetchone()
        print("You are connected to - ", db_version,"\n")
    
    except (Exception, psychopg2.Error) as error:
        print("Error while connectiong to postgreSQL", error)
    
    
        # Drop existing wine database
    try:
        cursor.execute('''DROP DATABASE wine;''')
        print ('Database dropped')
    except:
        print ('Database does not exist')
    con.commit()    

    # Create wine Database
    try:
        cursor.execute('''CREATE DATABASE wine;''')
        print("Database created")
    except:
        print('Database already exists')
    con.commit()
    
    # Close connection to the general database
    con.close()
    
    
    # 2) connect to wine database, create databases
    
    try:
        con = psycopg2.connect(database='wine', user='postgres', password=mypass, host='127.0.0.1', port='5432')
        print('wine database opened successfully')
        cursor = con.cursor()
    
    except (Exception, psychopg2.Error) as error:
        print("Error while connectiong to wine database", error)
        
    # Create table red_wine
    cursor.execute('''CREATE TABLE red_wine (
        "id" SMALLINT,
        "fixed_acidity" FLOAT(2),
        "volatile_acidity" FLOAT(3),
        "citric_acid" FLOAT(3),
        "residual_sugar" FLOAT(1),
        "chlorides" FLOAT(4),
        "free_sulfur_dioxide" FLOAT(4),
        "total_sulfur_dioxide" Float(1),
        "density" FLOAT(4),
        "ph" FLOAT(2),
        "sulphates" FLOAT(2),
        "alcohol" FLOAT(1),
        "quality" SMALLINT,
        "type" SMALLINT,
        PRIMARY KEY ("id")
        );''')
    print("red wine table created successfully")

    con.commit()
    
    # Create table white_wine
    cursor.execute('''CREATE TABLE white_wine (
        "id" SMALLINT,
        "fixed_acidity" FLOAT(2),
        "volatile_acidity" FLOAT(3),
        "citric_acid" FLOAT(3),
        "residual_sugar" FLOAT(1),
        "chlorides" FLOAT(4),
        "free_sulfur_dioxide" FLOAT(4),
        "total_sulfur_dioxide" Float(1),
        "density" FLOAT(4),
        "ph" FLOAT(2),
        "sulphates" FLOAT(2),
        "alcohol" FLOAT(1),
        "quality" SMALLINT,
        "type" SMALLINT,
        PRIMARY KEY ("id")
        );''')
    print("white wine table created successfully")

    con.commit()
    
    # Create table all_wine
    cursor.execute('''CREATE TABLE all_wine (
        "id" SMALLINT,
        "fixed_acidity" FLOAT(2),
        "volatile_acidity" FLOAT(3),
        "citric_acid" FLOAT(3),
        "residual_sugar" FLOAT(1),
        "chlorides" FLOAT(4),
        "free_sulfur_dioxide" FLOAT(4),
        "total_sulfur_dioxide" Float(1),
        "density" FLOAT(4),
        "ph" FLOAT(2),
        "sulphates" FLOAT(2),
        "alcohol" FLOAT(1),
        "quality" SMALLINT,
        "type" SMALLINT,
        PRIMARY KEY ("id")
        );''')
    print("all wine table created successfully")

    con.commit()
    
    # 3) load data into database

    # Create a list of tupples from the dataframe values
    tuples = [tuple(x) for x in red_data.to_numpy()]
    # Comma-separated dataframe columns
    cols = ','.join(list(red_data.columns))

    # SQL quert to execute
    query  = "INSERT INTO %s(%s) VALUES(%%s,%%s,%%s, %%s, %%s,%%s,%%s, %%s, %%s,%%s,%%s, %%s, %%s, %%s)" % ('red_wine', cols)
    cursor = con.cursor()
    try:
        cursor.executemany(query, tuples)
        con.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        con.rollback()
        cursor.close()
        return 1
    print("execute_many() done")
    cursor.close()


    # Create a list of tupples from the dataframe values
    tuples = [tuple(x) for x in white_data.to_numpy()]
    # Comma-separated dataframe columns
    cols = ','.join(list(white_data.columns))

    # SQL quert to execute
    query  = "INSERT INTO %s(%s) VALUES(%%s,%%s,%%s, %%s, %%s,%%s,%%s, %%s, %%s,%%s,%%s, %%s, %%s, %%s)" % ('white_wine', cols)
    cursor = con.cursor()
    try:
        cursor.executemany(query, tuples)
        con.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        con.rollback()
        cursor.close()
        return 1
    print("execute_many() done")
    cursor.close()


        # Create a list of tupples from the dataframe values
    tuples = [tuple(x) for x in all_data.to_numpy()]
    # Comma-separated dataframe columns
    cols = ','.join(list(all_data.columns))

    # SQL quert to execute
    query  = "INSERT INTO %s(%s) VALUES(%%s,%%s,%%s, %%s, %%s,%%s,%%s, %%s, %%s,%%s,%%s, %%s, %%s, %%s)" % ('all_wine', cols)
    cursor = con.cursor()
    try:
        cursor.executemany(query, tuples)
        con.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        con.rollback()
        cursor.close()
        return 1
    print("execute_many() done")
    cursor.close()