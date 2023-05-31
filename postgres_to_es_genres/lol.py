from functools import wraps
import os

from dotenv import load_dotenv


load_dotenv()
import psycopg2

####################
# Define Decorator #
####################

def psycopg2_cursor(conn_info):
    """Wrap function to setup and tear down a Postgres connection while
    providing a cursor object to make queries with.
    """
    def wrap(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            try:
                with psycopg2.connect(**conn_info) as connection:
                    # cursor = connection.cursor()
                    with connection.cursor() as cursor:
                        # Call function passing in cursor
                        return_val = f(cursor, *args, **kwargs)
            except psycopg2.OperationalError as err:
                print('psycopg2.OperationalError')
                return_val = None
            except Exception as err:
                return_val = None
                connection.close()

            return return_val
        return wrapper
    return wrap

#################
# Example Usage #
#################



SCHEMA = 'content'
TRIGGER = 'modified'
CHUNK_SIZE = 5
DSL = {'dbname': os.environ.get('DB_NAME'),
       'user': os.environ.get('DB_USER'),
       'password': os.environ.get('DB_PASSWORD'),
       'host': os.environ.get('DB_HOST', '127.0.0.1'),
       'port': os.environ.get('DB_PORT', 5432),
       'options': '-c search_path=%s' % os.environ.get('PG_SCHEMA')}


@psycopg2_cursor(DSL)
def tester(cursor):
    """Test function that uses our psycopg2 decorator
    """
    cursor.execute('SELECT 1 + ')
    print(cursor.fetchone())
    return cursor.fetchall()


if __name__ == '__main__':
    print(tester())
