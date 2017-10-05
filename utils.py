import psycopg2 as pg
import pyodbc
import sys

#----------------------------------------------------------------------------#
# Open connection to postgres db
#----------------------------------------------------------------------------#
def init_db(myDBName, myPasswordString):
    connectString = "dbname={} user='postgres' host='localhost' password={}".format(myDBName, myPasswordString)
    try:
        conn = pg.connect(connectString)
        return conn
    except:
        sys.stderr.write("Unable to connect to db")
        sys.exit(1)

#----------------------------------------------------------------------------#
# Open odbc connection
#----------------------------------------------------------------------------#
def init_odbc(myDSName):
    try:
        conn = pyodbc.connect("DSN=" + myDSName)
        return conn
    except:
        sys.stderr.write("Unable to connect to ODBC Data Source")
        sys.exxit(1)
