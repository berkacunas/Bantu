import codecs
import pymssql

from sql.sqlite import connectionimpl as sqlite_connection
from sql.sqlite import schemaimpl as sqlite_schema

from sql.sqlserver import connectionimpl as sqlserver_connection
from sql.sqlserver import schemaimpl as sqlserver_schema

class SchemaClone:
    
    @staticmethod
    def sqlserver_to_sqlite(sqlserver_name, sqlserver_database, sqlite_path):
    
        conn_src = sqlserver_connection.get_trusted_connection(sqlserver_name, sqlserver_database)
        conn_dest = sqlite_connection.get_connection(sqlite_path)
        
        if not conn_src:
            print(f'Cannot connect into server: {sqlserver_name}, database{sqlserver_database}')
            return
        
        tables = sqlserver_schema.get_tables(conn_src)
        
        for table in tables:
            if sqlite_schema.is_table_exists(conn_dest,table):
                continue
            
            columns = sqlserver_schema.get_columns(conn_src, table)
            pk = sqlserver_schema.get_primary_key(conn_src, table)
            fks = sqlserver_schema.get_foreign_keys(conn_src, table)
            
            build_create_script = sqlite_schema.build_create_table_script(conn_dest, table, columns, pk, fks)
            
            try:
                sqlite_schema.exec_create_table(conn_dest, build_create_script)
                print(f'Table: {table} created.')
            except Exception as error:
                print(error)
                sqlite_schema.CREATION_DEQUE.append((build_create_script, table), )
        
        pop_left = True     # First queue, then deque, then queue again ...
        while True:
            dsize = len(sqlite_schema.CREATION_DEQUE)
            if (dsize == 0):
                break
            
            for i in range(dsize):
                if pop_left:
                    build_create_script, table = sqlite_schema.CREATION_DEQUE.popleft()
                else:    
                    build_create_script, table = sqlite_schema.CREATION_DEQUE.pop()
                    
                try:
                    sqlite_schema.exec_create_table(conn_dest, build_create_script)
                except Exception as error:
                    if pop_left:
                        sqlite_schema.CREATION_DEQUE.append((build_create_script, table), )
                    else:
                        sqlite_schema.CREATION_DEQUE.appendleft((build_create_script, table), )

            pop_left = not pop_left
            # sqlserver_name, sqlserver_database, sqlite_path
            
        print(f'Database structure from database: {sqlserver_database} at SQL Server: {sqlserver_name} to {sqlite_path} successfully cloned.')
 
    @staticmethod
    def sqlite_to_sqlserver(sqlite_path, sqlserver_name, sqlserver_database):
    
        conn_src = sqlite_connection.get_connection(sqlite_path)
        if not conn_src:
            print(f'Cannot connect to sqlite file: {sqlite_path}')
            return
        
        conn_dest = sqlserver_connection.get_trusted_connection(sqlserver_name)
        if not conn_dest:
            print(f'Cannot connect into SQL Server: {sqlserver_name}')
            return
        
        if (not sqlserver_schema.is_database_exists(conn_dest, sqlserver_database)):
            create_database_script = sqlserver_schema.generate_create_database_script(None, sqlserver_database)
        
            if not sqlserver_schema.exec_create_database(conn_dest, create_database_script):
                print(f'Cannot create database: {sqlserver_database} in SQL Server')
                return
        
        conn_dest = sqlserver_schema.get_trusted_connection(sqlserver_name, sqlserver_database)
        if not conn_dest:
            print(f'Cannot connect into database: {sqlserver_database} in SQL Server: {sqlserver_name}')
            return
        
        tables = sqlite_schema.get_tables(conn_src)
        for table in tables:
            if sqlserver_schema.is_table_exists(conn_dest, table):
                continue
            
            columns = sqlite_schema.get_columns(conn_src, table)
            pk = sqlite_schema.get_primary_key(conn_src, table)
            fks = sqlite_schema.get_foreign_keys(conn_src, table)
            
            build_create_script = sqlserver_schema.build_create_table_script(table, columns, pk, fks, build_fk_constraints=False)
            try:
                sqlserver_schema.exec_script(conn_dest, build_create_script)
                print(f'Table {table} created.')
                
            except pymssql.exceptions.OperationalError as o_error:
                msg = codecs.decode(o_error.args[1])
                # print(msg)
                
                if o_error.args[0] == 1767:
                    # (1767, b"Foreign key 'None' references invalid table 'dbo.Publisher'.DB-Lib error message 20018, severity 16:\nGeneral SQL Server error: Check messages from the SQL Server\nDB-Lib error message 20018, severity 16:\nGeneral SQL Server error: Check messages from the SQL Server\n")
                    sqlserver_schema.CREATION_DEQUE.append((build_create_script, table), )
                    continue
                if o_error.args[0] == 3902:
                    # ("Cannot commit transaction: (3902, b'The COMMIT TRANSACTION request has no corresponding BEGIN TRANSACTION.DB-Lib error message 20018, severity 16:\\nGeneral SQL Server error: Check messages from the SQL Server\\n')",)
                    raise
                if o_error.args[0] == 2714:
                    # (2714, b"There is already an object named 'None' in the database.DB-Lib error message 20018, severity 16:\nGeneral SQL Server error: Check messages from the SQL Server\nDB-Lib error message 20018, severity 16:\nGeneral SQL Server error: Check messages from the SQL Server\n")
                    continue
                if o_error.args[0] == 173:
                    # (173, b"The definition for column 'Data' must include a data type.DB-Lib error message 20018, severity 15:\nGeneral SQL Server error: Check messages from the SQL Server\n")
                    raise

            except pymssql.exceptions.ProgrammingError as p_error:
                # sqlserver.CREATION_DEQUE.append((build_create_script, table), )
                print(p_error)
                raise
            except pymssql.exceptions.IntegrityError as i_error:
                print(i_error)
                raise
            except AttributeError as a_error:
                print(a_error)
                raise
            except Exception as error:
                print(error)
                raise
            
        
        pop_left = True     # First queue, then deque, then queue again ... make it better.
        while True:
            dsize = len(sqlserver_schema.CREATION_DEQUE)
            if (dsize == 0):
                break
            
            for i in range(dsize):
                if pop_left:
                    build_create_script, table = sqlserver_schema.CREATION_DEQUE.popleft()
                else:    
                    build_create_script, table = sqlserver_schema.CREATION_DEQUE.pop()
                    
                try:
                    sqlserver_schema.exec_script(conn_dest, build_create_script)
                    print(f'Table {table} created.')
                except Exception as error:
                    if pop_left:
                        sqlserver_schema.CREATION_DEQUE.append((build_create_script, table), )
                    else:
                        sqlserver_schema.CREATION_DEQUE.appendleft((build_create_script, table), )

            pop_left = not pop_left

    @staticmethod
    def sqlite_to_sqlserver_add_constraints(sqlite_path, sqlserver_name, sqlserver_database):
        
        conn_src = sqlite_connection.get_connection(sqlite_path)
        if not conn_src:
            print(f'Cannot connect to sqlite file: {sqlite_path}')
            return
        
        conn_dest = sqlserver_schema.get_trusted_connection(sqlserver_name, sqlserver_database)
        if not conn_dest:
            print(f'Cannot connect into database: {sqlserver_database} in SQL Server: {sqlserver_name}')
            return
        
        tables = sqlserver_schema.get_tables(conn_dest)
        for table in tables:
            fks = sqlite_schema.get_foreign_keys(conn_src, table)
            if len(fks) == 0:
                continue
            
            build_create_script = sqlserver_schema.build_foreign_key_script(fks)
            try:
                sqlserver_schema.exec_script(conn_dest, build_create_script)
                print(f'Foreign key constrains in Table {table} added.')
                
            except Exception as error:
                print(error)
                raise
            