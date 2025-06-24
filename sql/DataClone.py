import codecs
import pymssql

from sql.sqlite import connectionimpl as sqlite_connection
from sql.sqlite import schemaimpl as sqlite_schema
from sql.sqlite import dataimpl as sqlite_data

from sql.sqlserver import connectionimpl as sqlserver_connection
from sql.sqlserver import schemaimpl as sqlserver_schema
from sql.sqlserver import dataimpl as sqlserver_data

class DataClone:
    
    def sqlserver_to_sqlite(sqlserver_name, sqlserver_database, sqlite_path):
        
        conn_src = sqlserver_connection.get_trusted_connection(sqlserver_name, sqlserver_database)
        conn_dest = sqlite_connection.get_connection(sqlite_path)
        
        if not conn_src:
            print(f'Cannot connect into server: {sqlserver_name}, database{sqlserver_database}')
            return
        
        tables = sqlserver_schema.get_tables(conn_src)
        for table in tables:
            rows = sqlserver_data.select_all(conn_src, table)
            if rows and len(rows) > 0:
                if sqlite_schema.is_table_exists(conn_dest, table):
                    num_of_rows = sqlite_data.insert_many(conn_dest, table, rows)
                    print(f"{num_of_rows} rows inserted into {table}.")
                
    
    def sqlite_to_sqlserver(sqlite_path, sqlserver_name, sqlserver_database):
        
        conn_src = sqlite_connection.get_connection(sqlite_path)
        conn_dest = sqlserver_connection.get_trusted_connection(sqlserver_name, sqlserver_database)
        
        if not conn_src:
            print(f'Cannot connect into source Sqlite database: {sqlite_path}')
            return
        
        tables = sqlite_schema.get_tables(conn_src)
        for table in tables:
            rows = sqlite_data.select_all(conn_src, table)
            if rows and len(rows) > 0:
                if sqlserver_schema.is_table_exists(conn_dest, table):
                    
                    try:
                        sqlserver_schema.enable_identity_insert(conn_dest, table, True)
                        num_of_rows = sqlserver_data.insert_one_by_one(conn_dest, table, rows)
                        # num_of_rows = sqlserver_data.insert_many(conn_dest, table, rows)
                        print(f"{num_of_rows} rows inserted into {table}.")
                        sqlserver_schema.enable_identity_insert(conn_dest, table, False)
                    
                    except pymssql.exceptions.OperationalError as o_error:
                        msg = codecs.decode(o_error.args[1])
                        print(msg)
                        
                        match o_error.args[0]:
                            case 173:
                                # (173, b"The definition for column 'Data' must include a data type.DB-Lib error message 20018, severity 15:\nGeneral SQL Server error: Check messages from the SQL Server\n")
                                break
                            case 545:
                                sqlserver_data.CREATION_DEQUE.append((table, rows), )
                                # (545, b"Explicit value must be specified for identity column in table 'CurrencyMovement' either when IDENTITY_INSERT is set to ON or when a replication user is inserting into a NOT FOR REPLICATION identity column.DB-Lib error message 20018, severity 16:\n
                            case 1767:
                                # (1767, b"Foreign key 'None' references invalid table 'dbo.Publisher'.DB-Lib error message 20018, severity 16:\nGeneral SQL Server error: Check messages from the SQL Server\nDB-Lib error message 20018, severity 16:\nGeneral SQL Server error: Check messages from the SQL Server\n")
                                sqlserver_data.CREATION_DEQUE.append((table, rows), )
                                continue
                            case 2714:
                                # (2714, b"There is already an object named 'None' in the database.DB-Lib error message 20018, severity 16:\nGeneral SQL Server error: Check messages from the SQL Server\nDB-Lib error message 20018, severity 16:\nGeneral SQL Server error: Check messages from the SQL Server\n")
                                continue
                            case 3902:
                                # ("Cannot commit transaction: (3902, b'The COMMIT TRANSACTION request has no corresponding BEGIN TRANSACTION.DB-Lib error message 20018, severity 16:\\nGeneral SQL Server error: Check messages from the SQL Server\\n')",)
                                break
                            case _:
                                break
                        
                    except pymssql.exceptions.ProgrammingError as p_error:
                        # sqlserver.CREATION_DEQUE.append((build_create_script, table), )
                        print(p_error)
                        # if p_error.args[0] > 0:
                        #     pass
                        
                    except pymssql.exceptions.IntegrityError as i_error:
                        print(i_error)
                        
                    except Exception as error:
                        print(error)
