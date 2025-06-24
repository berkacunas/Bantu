from datetime import datetime
from collections import deque

from pymssql import OperationalError, ProgrammingError, IntegrityError

from sql.Interfaces import Column, Foreign_Key, sqlserver_to_sqlite_types_dict
from sql.sqlserver.connectionimpl import get_connection, get_trusted_connection
from bdatetime.bdatetime import is_julian

SYSTEM_DATABASES = ('master', 'model', 'tempdb', 'msdb')
CREATION_DEQUE = deque()

def db_structure(server_ip, user, pwd):
    
    conn = get_connection(server_ip, user, pwd)
    dbs = _get_schema(conn)
    
    return dbs
      
def db_structure_trusted_conn(server):
    
    conn = get_trusted_connection(server=server)    
    DBs = _get_schema(conn)
    print(DBs)
   
def is_database_exists(conn, database):
    
    sql = f"SELECT DB_ID('{database}');"
    
    cursor = conn.cursor()
    cursor.execute(sql)
    row = cursor.fetchone()
    
    return row and row[0]

def get_databases(conn):
    
    DBs = DBs = []
    sql = "SELECT name, database_id, create_date FROM sys.databases;"
    
    cursor = conn.cursor()
    cursor.execute(sql)
    row = cursor.fetchall()
    
    for DB in row:
        if DB[0] in SYSTEM_DATABASES:
            continue
        DBs.append(DB)
    
    conn.close()
    
    return DBs

def is_table_exists(conn, table):
    
    sql = f"SELECT OBJECT_ID(N'dbo.{table}', N'U');"
    
    cursor = conn.cursor()
    cursor.execute(sql)
    row = cursor.fetchone()
    
    return row and row[0]

def get_tables(conn):
    
    tables = []
    sql = "SELECT name FROM sys.tables;"
    
    cursor = conn.cursor()
    
    cursor.execute(sql)
    rows = cursor.fetchall()

    for table in rows:
        tables.append(table[0])
    
    return tables

def get_columns(conn, table_name):
    
    columns = []
    sql = ''' SELECT TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, IS_NULLABLE, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, DATETIME_PRECISION
              FROM INFORMATION_SCHEMA.COLUMNS 
              WHERE TABLE_NAME = %s;
          '''
    
    cursor = conn.cursor()
    try:
        cursor.execute(sql, [table_name])
        row = cursor.fetchall()

        for item in row:
            # Adding values
            column = Column(
                TABLE_NAME=item[0],    
                COLUMN_NAME=item[1], 
                ORDINAL_POSITION=item[2], 
                IS_NULLABLE=item[3] != 'NO', 
                DATA_TYPE=item[4], 
                CHARACTER_MAXIMUM_LENGTH=item[5], 
                DATETIME_PRECISION=item[6],
                IS_PK=None,             # This doesn't exist in Sql Server
                DEFAULT_VALUE=None      # This doesn't exist in Sql Server
                )
            
            columns.append(column)
            
        return columns
    
    except Exception as e:
        print(e)

def get_primary_key(conn, table_name):
    
    primary_key = None
    sql = '''
            SELECT K.COLUMN_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS T 
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE K
            ON K.CONSTRAINT_NAME=T.CONSTRAINT_NAME  
            WHERE  K.TABLE_NAME= %s
            AND T.CONSTRAINT_TYPE='PRIMARY KEY';
    '''

    cursor = conn.cursor()
    try:
        cursor.execute(sql, [table_name])
        row = cursor.fetchone()

        if row:
            primary_key = row[0]
            
            
        return primary_key
    
    except Exception as e:
        print(e)
    
def get_foreign_keys(conn, table_name):
    
    foreign_keys = []
    
    sql = '''
            SELECT KCU1.TABLE_NAME AS ReferencedTableName,    
                KCU1.COLUMN_NAME AS ReferencedColumnName,    
                KCU2.TABLE_NAME AS ReferencingTableName,    
                KCU2.COLUMN_NAME AS ReferencingColumnName,
                KCU2.CONSTRAINT_NAME AS ConstraintName
            FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS AS RC
            INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KCU1 ON RC.UNIQUE_CONSTRAINT_NAME= KCU1.CONSTRAINT_NAME
            INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KCU2 ON RC.CONSTRAINT_NAME  = KCU2.CONSTRAINT_NAME
            WHERE KCU2.TABLE_NAME = %s;
        '''
    cursor = conn.cursor()
    cursor.execute(sql, [table_name])
    row = cursor.fetchall()

    for item in row:
        fk = Foreign_Key(
            REFERENCED_TABLE_NAME=item[0], 
            REFERENCED_COLUMN_NAME=item[1], 
            REFERENCING_TABLE_NAME=item[2], 
            REFERENCING_COLUMN_NAME=item[3],
            CONSTRAINT_NAME=item[4], 
            ID=None,                            # NOT EXISTS
            SEQ=None,                           # NOT EXISTS
            ON_UPDATE=None,                     # NOT EXISTS
            ON_DELETE=None,                     # NOT EXISTS
            MATCH=None                          # NOT EXISTS
            ) 
        if fk.REFERENCING_TABLE_NAME == table_name:
            foreign_keys.append(fk)
        
    return foreign_keys

def enable_identity_insert(conn, table_name, enable):
    
    switch = 'OFF'
    if enable:
        switch = 'ON'
        
    sql = f'SET IDENTITY_INSERT [{table_name}] {switch}'
    
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
    except Exception as error:
        print(error)
        raise

def enable_foreign_keys(conn, table_name):
    
    fks = get_foreign_keys(conn, table_name)
    
    for fk in fks:
        enable_foreign_key(conn, table_name, fk.CONSTRAINT_NAME)

def disable_foreign_keys(conn, table_name):
    
    fks = get_foreign_keys(conn, table_name)
    
    for fk in fks:
        disable_foreign_key(conn, table_name, fk.CONSTRAINT_NAME)

def enable_foreign_key(conn, table_name, constraint_name):
    
    return _exec_enable_foreign_key(conn, table_name, constraint_name, True)

def disable_foreign_key(conn, table_name, constraint_name):
    
    return _exec_enable_foreign_key(conn, table_name, constraint_name, False)

def _exec_enable_foreign_key(conn, table_name, constraint_name, enable):
    
    sql = None
    if enable:
        sql = f'ALTER TABLE {table_name} CHECK CONSTRAINT {constraint_name};'
    else:
        sql = f'ALTER TABLE {table_name} NOCHECK CONSTRAINT {constraint_name};'
        
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
    except Exception as error:
        print(error)
        raise

def is_foreign_key_trusted(conn, constraint_name):
    
    sql = "SELECT o.name, fk.name, fk.is_not_trusted, fk.is_disabled FROM sys.foreign_keys AS fk "
    sql += f"INNER JOIN sys.objects AS o ON fk.parent_object_id = o.object_id WHERE fk.name = '{constraint_name}";

    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        
        row = cursor.fetchone()
        if row:
            return not bool(row[2])
        
        raise Exception('is_foreign_key_trusted query returns empty.')
    except Exception as error:
        print(error)
        raise

def set_foreign_key_trusted_with_check(conn, table, constraint_name):
    
    sql = f'ALTER TABLE {table} WITH CHECK CHECK CONSTRAINT {constraint_name}';

    cursor = conn.cursor()
    try:
        cursor.execute(sql)
    except Exception as error:
        print(error)
        raise
     
def generate_create_table_script(database, table, columns, primary_key, foreign_keys):
    
    script = f'USE {database}' \
        '\n' \
        'GO\n\n' \
        f'/****** Object:  Table [dbo].[{table}]    Script Date: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')} ******/' + '\n' \
        'SET ANSI_NULLS ON\n' \
        'GO\n\n' \
        'SET QUOTED_IDENTIFIER ON\n' \
        'GO\n\n' \
        f'CREATE TABLE [dbo].[{table}](' + '\n'
            
    i = 0
    for column in columns:
        script += '\t' + f'[{column.COLUMN_NAME}] [{column.DATA_TYPE}]'
        
        if column.COLUMN_NAME == primary_key:
            script += ' IDENTITY(1,1) '
        
        if column.DATA_TYPE == 'nvarchar' or column.DATA_TYPE == 'varchar' or column.DATA_TYPE == 'varbinary':
            if column.CHARACTER_MAXIMUM_LENGTH == -1:
                script += f'(max) '
            else:
                script += f'({column.CHARACTER_MAXIMUM_LENGTH}) '
        elif column.DATA_TYPE == 'datetime2':
            script += f'({column.DATETIME_PRECISION}) '
        elif column.DATA_TYPE == 'int':
            script += ' '   # empty space
        
        if column.IS_NULLABLE == 'NO':
            script += 'NOT NULL'
        elif column.IS_NULLABLE == 'YES':
            script += 'NULL'
            
        if i < len(columns) - 1:
            script += ','
            
        script += '\n'
        i += 1

    script += f'CONSTRAINT [PK_{table}] PRIMARY KEY CLUSTERED' + '\n' \
        '(' + '\n' \
        f'[{primary_key}] ASC\n' \
        ')WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]\n' \
        ') ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]\n' \
        'GO\n\n'
        
    for fk in foreign_keys:
        script += f'ALTER TABLE [dbo].[{fk.REFERENCING_TABLE_NAME}]  WITH CHECK ADD  CONSTRAINT [{fk.CONSTRAINT_NAME}] FOREIGN KEY([{fk.REFERENCING_COLUMN_NAME}])' + '\n' \
            f'REFERENCES [dbo].[{fk.REFERENCED_TABLE_NAME}] ([{fk.REFERENCED_COLUMN_NAME}])' + '\n' \
            'GO\n\n' \
            f'ALTER TABLE [dbo].[{fk.REFERENCING_TABLE_NAME}] CHECK CONSTRAINT [{fk.CONSTRAINT_NAME}]' + '\n' \
            'GO\n\n'
    
    return script

def build_create_table_script(table, columns, primary_key, foreign_keys, build_fk_constraints=True):

    script =  f'CREATE TABLE [dbo].[{table}] (\n'
    
    for column in columns:
        script += '\t' + f'[{column.COLUMN_NAME}] '
        
        sqlserver_type = sqlserver_to_sqlite_types_dict[column.DATA_TYPE]
        
        if len(sqlserver_type) > 1:
            # Codeflow never drops here if all lists in dict have 1 element. 
            # When they have more elements in the future, this block is for decide the correct type for conversion.
            pass    
        
        sqlserver_type = sqlserver_type[0]
        script += f'{sqlserver_type} '
        
        if column.COLUMN_NAME == primary_key:
            script += ' IDENTITY(1,1) '
        
        if column.IS_NULLABLE:
            script += 'NULL'
        else:
            script += 'NOT NULL'
            
        script += ',\n'

    if primary_key:
        script += f'CONSTRAINT [PK_{table}] PRIMARY KEY CLUSTERED ([{primary_key}] ASC)\n'
    else:
        script = script[0:len(script) - 2]
        
    script += ');\n' 
    
    if build_fk_constraints:
        script += build_foreign_key_script(foreign_keys)
        
    return script

def build_foreign_key_script(foreign_keys) -> str:
    
    script = ""
    for fk in foreign_keys:
        script = f'ALTER TABLE [dbo].[{fk.REFERENCING_TABLE_NAME}]  WITH CHECK ADD  CONSTRAINT [{fk.CONSTRAINT_NAME}] FOREIGN KEY([{fk.REFERENCING_COLUMN_NAME}])' + '\n'
        script += f'REFERENCES [dbo].[{fk.REFERENCED_TABLE_NAME}] ([{fk.REFERENCED_COLUMN_NAME}]);' + '\n\n'
        script += f'ALTER TABLE [dbo].[{fk.REFERENCING_TABLE_NAME}] CHECK CONSTRAINT [{fk.CONSTRAINT_NAME}];' + '\n'

    return script

def move_file(conn, database_name, new_filename):   # Not working, need fix in filenames.
    
    new_filename += fr'\{database_name}'
    cursor = conn.cursor()
    
    sql = f"ALTER DATABASE %s MODIFY FILE (NAME = %s, FILENAME = N'%s\');"

    cursor.execute(sql, (database_name, database_name, new_filename))
    
    # This action requires exclusive access to the database. If another connection is open to the database, 
    # the ALTER DATABASE statement is blocked until all connections are closed. 
    sql = f'ALTER DATABASE {database_name}'
    sql += 'SET OFFLINE;'
    cursor.execute(sql)
    
    sql = 'ALTER DATABASE {database_name}'
    sql += 'SET OFFLINE'
    sql += 'WITH ROLLBACK IMMEDIATE;'
    cursor.execute(sql)
    
    sql = f"ALTER DATABASE {database_name}"
    sql += "SET ONLINE;"
    cursor.execute(sql)
    
    # conn.commit()
    
    sql = f'SELECT name, physical_name AS CurrentLocation, state_desc FROM sys.master_files WHERE database_id = DB_ID({database_name});'
    cursor.execute(sql)
    
    rows = cursor.fetchall()
    
    if rows:
        for row in rows:
            print(row)

def generate_create_database_script(directory, database):
    
    return f'CREATE DATABASE [{database}]'
            
def exec_create_database(conn, script) -> bool:
    
    conn.autocommit(True)
    cursor = conn.cursor()
    
    try:
        cursor.execute(script)
        return True
    except Exception as error:
        print(error)
        return False
    finally:
        conn.autocommit(False)

def exec_script(conn, script):
    
    cursor = conn.cursor()
    try:
        cursor.execute(script)
        conn.commit()
    except OperationalError as o_error:
        print(f'SQL: {script} | Error: {o_error}')
        conn.rollback()
        raise
    except ProgrammingError as p_error:
        print(f'SQL: {script} | Error: {p_error}')
        conn.rollback()
        raise
    except IntegrityError as i_error:
        print(f'SQL: {script} | Error: {i_error}')
        conn.rollback()
        raise
    except Exception as error:
        print(f'SQL: {script} | Error: {error}')
        conn.rollback()
        raise
    
def _get_schema(conn):
    
    DBs = []
    sql = "SELECT name FROM master.dbo.sysdatabases;"
    
    cursor = conn.cursor()
    cursor.execute(sql)
    row = cursor.fetchall()

    for DB in row:
        if DB[0] in SYSTEM_DATABASES:
            continue
        DBs.append(DB[0])
    
    conn.close()
    return DBs
