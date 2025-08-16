import snowflake.connector
from snowflake.connector.errors import ProgrammingError, DatabaseError

def check_snowflake_connection():
    try:
        # Create a connection object
        conn = snowflake.connector.connect(
        user="Uddhav18",
        password="UddhavGavhane@18",
        account="apgazqy-ex48851",
        warehouse="developer",
        database="db_dev",
        schema="landing"
        )
    

        # Create a cursor object
        cur = conn.cursor()

        # Run a simple query to check connection
        cur.execute("SELECT CURRENT_VERSION()")
        version = cur.fetchone()
        print(f"✅ Successfully connected to Snowflake")

    except (ProgrammingError, DatabaseError) as e:
        print(f"❌ Failed to connect: {e}")
    finally:
        try:
            cur.close()
            conn.close()
        except:
            pass


if __name__ == "__main__":
    check_snowflake_connection()