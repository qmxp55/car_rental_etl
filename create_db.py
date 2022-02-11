import sqlite3
from sqlite3 import Error

def create_db(db_file):
    """ create a database connection to a SQLite database """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        sql_file = open('db_project.sql')
        sql_as_string = sql_file.read()
        cursor.executescript(sql_as_string)

        print('Database %s was successfully created' %(db_file))

    except Error as e:
        print(e)
    finally:
        if conn:
            conn.close()


if __name__ == '__main__':
    create_db(r'rentals.db')