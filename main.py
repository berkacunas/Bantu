import argparse

from sql.SchemaClone import SchemaClone
from sql.DataClone import DataClone
    

def execute(source_db_type: str, dest_db_type: str, 
            mssql_trusted: bool = True, mssql_server_name: str = None, mssql_database_name: str = None, mssql_username: str = None, mssql_password: str = None,
            sqlite_path: str = None, schema_clone: bool = True, data_clone: bool = False):
    
    if not mssql_trusted:
        raise NotImplementedError("SQL-Server connection without trusted-connection is not implemented yet.")
        
    if source_db_type == "mssql" and dest_db_type == "sqlite":
        if schema_clone:
            SchemaClone.sqlserver_to_sqlite(sqlserver_name=mssql_server_name, sqlserver_database=mssql_database_name, 
                                            sqlserver_username=mssql_username, sqlserver_password=mssql_password, 
                                            sqlite_path=sqlite_path, mssql_trusted=False)
        if data_clone:
            DataClone.sqlserver_to_sqlite(mssql_server_name, mssql_database_name, sqlite_path)
        
    elif source_db_type == "sqlite" and dest_db_type == "mssql":
        if schema_clone:
            SchemaClone.sqlite_to_sqlserver(sqlite_path, mssql_server_name, mssql_database_name)
        if data_clone:
            DataClone.sqlite_to_sqlserver(sqlite_path, mssql_server_name, mssql_database_name)
            SchemaClone.sqlite_to_sqlserver_add_constraints(sqlite_path, mssql_server_name, mssql_database_name)

    else:
        print("No-clone option selected.")

def main():
    
    parser = argparse.ArgumentParser(prog="Sql Schema/Data Migrate", 
                                     usage='%(prog)s [options]',
                                     description="Migrate schema and/or data from one database to another.",
                                     epilog="Thanks for using %(prog)s",
                                     add_help=False)
    
    required_args_parser = parser.add_argument_group('required arguments')
    required_args_parser.add_argument("-db-type1", type=str, choices=["mssql", "sqlite"], required=True, help='Choose source database type')
    required_args_parser.add_argument("-db-type2", type=str, choices=["mssql", "sqlite"], required=True, help="Choose destination database type")
    
    optional_args_parser = parser.add_argument_group('optional arguments')
    optional_args_parser.add_argument("-mt", "--mssql-trusted", dest="mssql_trusted", action="store_true", default=False, help="Connect MSSQL with trusted connection")
    optional_args_parser.add_argument("-mssql-server-name", type=str, help="SQL Server name for connection")
    optional_args_parser.add_argument("-mssql-database-name", type=str, help="SQL Server database name for migration")
    optional_args_parser.add_argument("-mssql-username", type=str, help="SQL Server database username for login")
    optional_args_parser.add_argument("-mssql-password", type=str, help="SQL Server database password for login")
    
    optional_args_parser.add_argument("-sqlite-path", type=str, help="SQLite Database File")
    
    optional_args_parser.add_argument("-schema-clone", action="store_true", default=False, help="Clone schema")
    optional_args_parser.add_argument("-data-clone", action="store_true", default=False, help="Clone data")
    
    try:
        args = parser.parse_args()
        execute(source_db_type=args.db_type1, 
                dest_db_type=args.db_type2, 
                mssql_server_name=args.mssql_server_name, 
                mssql_database_name=args.mssql_database_name, 
                mssql_username=args.mssql_username,
                mssql_password=args.mssql_password,
                sqlite_path=args.sqlite_path, 
                mssql_trusted=args.mssql_trusted,
                schema_clone=args.schema_clone, 
                data_clone=args.data_clone)
        
    except Exception as error:
        print(f"ArgParse Error | {error}")
    

if __name__ == '__main__':
    
    main()
