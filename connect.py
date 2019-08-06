import pyodbc

def hadoop():
    pyodbc.autocommit = True
    pyodbc.pooling = False

    print ('Connecting Hadoop...')
    conn = pyodbc.connect("DSN=hive64", autocommit=True)
    # cursor = conn.cursor()
    print ('Hadoop Connected')
    return conn

def local():
    pyodbc.autocommit = True
    pyodbc.pooling = False

    print ('Connecting Local MySQL...')
    conn = pyodbc.connect("DSN=ubuntu-mariadb-dev", autocommit=True)
    # cursor = conn.cursor()
    print ('Local MySQL Connected')
    return conn

