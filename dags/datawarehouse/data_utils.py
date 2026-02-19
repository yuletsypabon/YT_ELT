from airflow.providers.postgres.hooks.postgres import PostgresHook
from pyscopg2.extras import RealDictCursor  

def get_connection_cursor():
    hook = PostgresHook(postgres_connection_id="postgres_db_yt_elt", database="elt_db")
    connection = hook.get_connection()
    cur = connection.cursor(cursor_factory=RealDictCursor)
    return cur, connection
    
    # cur.execute("SELECT * FROM video_stats")      
    
    
def close_connection_cursor(cur, connection):
    cur.close()
    connection.close()