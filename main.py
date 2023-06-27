import os 
from dotenv import load_dotenv
import requests 
import pandas as pd 
import psycopg2 


# Get the data
url = 'https://jsonplaceholder.typicode.com/users' 
response = requests.get(url)
json_response = response.json() 
df = pd.json_normalize(json_response) 

#Extract column names for table schema
df.columns = [item.lower().replace(" ", "_").replace(".", "_") for item in df.columns]

#Map pandas data types to SQL data types
replacements = {
    'object': 'varchar',
    'float64': 'float',
    'int64': 'int',
    'datetime64': 'timestamp',
    'timedelta64[ns]': 'varchar'
}

#Create table schema
col_str = ", ".join("{} {}".format(n, d) for (n, d) in zip(df.columns,df.dtypes.replace(replacements)))

#Save df to csv
df.to_csv('post_users.csv', header=df.columns, index=False, encoding='UTF8')
print("csv file created")

#Open connection, create table, open the csv file and copy its contents to table
load_dotenv()
conn = None
try:
    conn_str = psycopg2.connect(
        host = os.getenv("HOST"),
        dbname = os.getenv("DBNAME"),
        user = os.getenv("USER"),
        port = os.getenv("PORT"),
        password = os.getenv("PASSWORD")
    )
    
    with conn_str as conn:
        print('PostgreSQL server information')
        print(conn.get_dsn_parameters(), "\n")

        with conn.cursor() as cur:
            print('Database opened succefully')

            cur.execute("DROP TABLE IF EXISTS post_users_test")

            cur.execute("CREATE TABLE post_users_test (%s)" % col_str)
            print("Table created successfully")

            csv_file = open("post_users.csv")
            print("file opened in memory")
            SQL = """COPY post_users_test FROM STDIN WITH CSV HEADER DELIMITER AS ','"""
            cur.copy_expert(sql=SQL, file=csv_file)
            conn.commit()
            print("file copied to db")

            #check if the data has been uploaded
            cur.execute("SELECT * FROM post_users_test")
            for row in cur.fetchall():
                print(row)

except (Exception, psycopg2.DatabaseError) as error:
    print(error)

finally: 
    if conn is not None: 
        conn.close()
print("PostgreSQL connection is closed")
print('table post_users_test import completed')




