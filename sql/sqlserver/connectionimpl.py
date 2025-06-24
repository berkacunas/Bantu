import pymssql

def get_connection(server, user, pwd, database=None):
    
    conn = pymssql.connect(host=server, user=user, password=pwd, database=database)
    return conn

def get_trusted_connection(server, database=None):
    
    conn = pymssql.connect(server=server, database=database)  
    return conn
