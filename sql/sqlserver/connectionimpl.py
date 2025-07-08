import pymssql

def get_connection(server, user, pwd, database=None):
    
    conn = None
    try:
        conn = pymssql.connect(host=server, user=user, password=pwd, database=database)
    except Exception as error:
        raise error
    
    return conn

def get_trusted_connection(server, database=None):
    
    conn = None
    
    try:
        conn = pymssql.connect(server=server, database=database)
    except Exception as error:
        raise error
    
    return conn
