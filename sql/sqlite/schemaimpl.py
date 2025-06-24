from collections import deque

from sql.sqlite.connectionimpl import get_connection
from sql.Interfaces import Column, Foreign_Key, sqlite_to_sqlserver_types_dict

_system_databases = ('sqlite_sequence')
CREATION_DEQUE = deque()

def is_table_exists(conn, table):
    
    sql = f"SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='{table}';"
    
    cursor = conn.cursor()
    cursor.execute(sql)
    row = cursor.fetchone()
    if row and len(row) > 0:
        return int(row[0]) > 0
    else:
        return False
    
def get_tables(conn):

    tables = []    
    cmd = "SELECT name FROM sqlite_master WHERE type='table';"

    curr = conn.cursor()
    curr.execute(cmd)
    rows = curr.fetchall()
    
    for table in rows:
        tables.append(table[0])
        
    tables.remove(_system_databases)
    
    return tables

def get_columns(conn, table_name):
    
    columns = []
    cmd = 'SELECT * FROM pragma_table_info(?);'     # f'PRAGMA table_info({table_name});'
    
    curr = conn.cursor()
    curr.execute(cmd, (table_name, ))
    rows = curr.fetchall()
    
    for item in rows:
        is_nullable = item[3] != 1      # item[3] iş isnotnull. I convert it into is_nullable for consistency with other databases.
        is_pk = item[5] == 1

        column = Column(                    # table_info            # converted
            TABLE_NAME=table_name,          # NOT-EXISTS            TABLE_NAME
            ORDINAL_POSITION=item[0],       # cid                   ORDINAL_POSITION
            COLUMN_NAME=item[1],            # name                  COLUMN_NAME
            DATA_TYPE=item[2],              # type                  DATA_TYPE
            IS_NULLABLE=is_nullable,        # notnull     =not=     IS_NULLABLE
            DEFAULT_VALUE=item[4],          # dflt_value            DEFAULT_VALUE
            IS_PK=is_pk,                    # pk                    IS_PK
            CHARACTER_MAXIMUM_LENGTH=None,  # NOT EXISTS
            DATETIME_PRECISION=None         # NOT EXISTS
            )
        columns.append(column)
        
    return columns

def get_primary_key(conn, table_name):
    
    pk_column = None
    cmd = 'SELECT name FROM pragma_table_info(?) WHERE pk = 1';
    
    curr = conn.cursor()
    curr.execute(cmd, (table_name, ))
    rows = curr.fetchall()
    
    for row in rows:
        pk_column = row[0]
        
    return pk_column

def get_foreign_keys(conn, table_name):
    
    foreign_keys = []
    cmd = 'SELECT * FROM pragma_foreign_key_list(?);'
    
    cursor = conn.cursor()
    cursor.execute(cmd, [table_name])
    row = cursor.fetchall()

    for item in row:
        fk = Foreign_Key(                                   # foreign_key_list      # converted
            ID=item[0],                                     # id                    ID
            SEQ=item[1],                                    # seq                   SEQ
            REFERENCING_TABLE_NAME=table_name,              # NOT EXISTS            REFERENCING_TABLE_NAME
            REFERENCED_TABLE_NAME=item[2],                  # table                 REFERENCED_TABLE_NAME
            REFERENCING_COLUMN_NAME=item[3],                # from                  REFERENCING_COLUMN_NAME
            REFERENCED_COLUMN_NAME=item[4],                 # to                    REFERENCED_COLUMN_NAME
            ON_UPDATE=item[5],                              # on_update             ON_UPDATE
            ON_DELETE=item[6],                              # on_delete             ON_DELETE
            MATCH=item[7],                                  # match                 MATCH
            CONSTRAINT_NAME=f'FK_{table_name}_{item[3]}'    # NOT EXISTS
            )
        foreign_keys.append(fk)
        
    return foreign_keys

def get_create_table_script(conn, table_name):
    
    script = ""
    cmd = "SELECT sql FROM sqlite_master WHERE type='table' AND tbl_name = ?;"
    
    curr = conn.cursor()
    curr.execute(cmd, (table_name, ))
    rows = curr.fetchall()
    
    if rows:
        for row in rows:
            script = row[0]
            
    return script

def build_create_table_script(conn, table_name, columns:Column, primary_key=None, foreign_keys=None) -> str:
    
    sqlite_types = sqlite_to_sqlserver_types_dict.keys()   # ["TEXT", "INTEGER", "REAL", "NUMERIC", "BLOB"]
    
    script = f'CREATE TABLE "{table_name}" (' + '\n'
    
    for column in columns:
        script += '\t' + f'"{column.COLUMN_NAME}" '
        
        for sqlite_type in sqlite_types:
            sqlserver_type = sqlite_to_sqlserver_types_dict[sqlite_type]
            if column.DATA_TYPE in sqlserver_type:  # e.g. nvarchar in nvarchar(max)
                script += f'{sqlite_type} '
                break
        
        if column.IS_NULLABLE:
            script += "NULL"
        else:
            script += "NOT NULL"
            
        if column.COLUMN_NAME == primary_key:
            script += " UNIQUE"

        script += ',\n'
    
    if (len(foreign_keys) == 0) and (not primary_key):
        script = script[0:len(script) - 2]
      
    for fk in foreign_keys:
        script += f'FOREIGN KEY({fk.REFERENCING_COLUMN_NAME}) REFERENCES {fk.REFERENCED_TABLE_NAME}({fk.REFERENCED_COLUMN_NAME}),' + '\n'
        
    if primary_key:
        script += f'PRIMARY KEY("{primary_key}" AUTOINCREMENT)'
    
    script += '\n' + ')'
        
    return script

def exec_create_database(path) -> bool:
    
    conn = get_connection(path)
    return conn != None

def exec_create_table(conn, script):
    
    cursor = conn.cursor()
    try:
        cursor.execute(script)
        conn.commit()
    except Exception as error:
        conn.rollback()
        raise
 