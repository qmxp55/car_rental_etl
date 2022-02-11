import sqlite3
from sqlite3 import Error
import pandas as pd

def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by the db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except Error as e:
        print(e)

    return conn

def create_table(conn, create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(''' DROP table IF EXISTS TOPBOOKINGS;
                  ''')
        c.execute(create_table_sql)
    except Error as e:
        print(e)

def create_topbooking(conn, topbooking):
    """
    Create a new entry into the topbookings table
    :param conn:
    :param topbooking:
    :return: project id
    """
    sql = ''' INSERT INTO TOPBOOKINGS(REG_NUM, MODEL_NAME, MAKE, MODEL_YEAR, AVG_RENT_TIME, NUMBER_BOOKINGS)
              VALUES(?,?,?,?,?,?) '''

    cur = conn.cursor()
    cur.execute(sql, topbooking)
    conn.commit()
    
    return cur.lastrowid


def query_save_topbookings(conn):
    """
    Perform query on DB ta and save output to TOPBOOKIGS table
    :param conn: the Connection object
    :return:
    """
    cur = conn.cursor()
    cur.execute('''SELECT REG_NUM, MODEL_NAME, MAKE, MODEL_YEAR, ROUND(AVG(ABS(JULIANDAY(RET_DT_TIME) - JULIANDAY(FROM_DT_TIME))), 1), COUNT(*) as NBOOKINGS FROM BOOKING_DETAILS as bd
                   JOIN CAR AS c ON c.REGISTRATION_NUMBER == bd.REG_NUM
                   GROUP BY REG_NUM
                   ORDER BY NBOOKINGS DESC ''')

    rows = cur.fetchall()

    for row in rows:
        create_topbooking(conn, row)

    print(pd.DataFrame(rows, columns=['REG_NUM', 'MODEL_NAME', 'MAKE', 'MODEL_YEAR', 'AVG_RENT_TIME', 'NUMBER_BOOKINGS']))

def main():

    # database filename
    database = r'rentals.db'

    sql_create_topbookings_table = """  CREATE TABLE TOPBOOKINGS (
                                        id integer PRIMARY KEY,
                                        REG_NUM CHAR(7) NOT NULL,
                                        MODEL_NAME VARCHAR(25) NOT NULL,
                                        MAKE VARCHAR(25) NOT NULL,
                                        MODEL_YEAR NUMBER(4) NOT NULL,
                                        AVG_RENT_TIME NUMBER(3,1) NOT NULL,
                                        NUMBER_BOOKINGS INTEGER NOT NULL
                                    ); """

    # create a database connection
    conn = create_connection(database)

    if conn is not None:

        # create TOPBOOKIGS table
        create_table(conn, sql_create_topbookings_table)

        # query and save output to TOPBOOKIGS table
        query_save_topbookings(conn)

    else:
        print("Error! cannot create the database connection.")


if __name__ == '__main__':
    main()
