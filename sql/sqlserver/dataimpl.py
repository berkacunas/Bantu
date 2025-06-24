from datetime import datetime
from collections import deque
import decimal
import codecs
import pymssql

from sql.sqlserver.schemaimpl import get_columns, get_primary_key, get_foreign_keys
from bdatetime.bdatetime import is_valid_dt_format, is_julian, from_julian

CREATION_DEQUE = deque()

def select_all(conn, table):
    
    columns = get_columns(conn, table)
    length = len(columns)
    pk = None
    
    sql = "SELECT "
    for i in range(length):
        sql += f"[{columns[i].COLUMN_NAME}]"
        if i < length - 1:
            sql += ", "
        
    sql += f" FROM [{table}]"
        
    cursor = conn.cursor()
    
    rows = None
    try:
        cursor.execute(sql)
        rows = cursor.fetchall()
    except pymssql.OperationalError as op_error:
        print(op_error)
    except Exception as error:
        print(error)
        
    return rows

def insert_one_by_one(conn, table, rows, skip_primary_key=False) -> int:
    
    cursor = conn.cursor()
    rows_inserted = 0
    columns = get_columns(conn, table)
    length = len(columns)
    pk = None
    foreign_keys = get_foreign_keys(conn, table)
    
    if skip_primary_key:
        pk = get_primary_key(conn, table)
        
    for row in rows:
        sql = f"INSERT INTO [{table}] "
        
        sql += "("
        for i in range(length):
            if skip_primary_key and pk and columns[i].COLUMN_NAME == pk:
                continue
            sql += f"[{columns[i].COLUMN_NAME}]"
            if i < length - 1:
                sql += ", "
            else:
                sql += ") "
        
        sql += "VALUES("
        for i in range(length):
            if skip_primary_key and pk and columns[i].COLUMN_NAME == pk:
                continue
            else:
                sql += "%s"
                
            if i < length - 1:
                sql += ", "
            else:
                sql += ")"
        
        data_list = []
        for k in range(len(row)):
            if skip_primary_key and pk and columns[k].COLUMN_NAME == pk:
                continue
            
            val = row[k]
            if isinstance(val, str):
                if '\'' in val:
                    val = val.replace('\'', '\'\'')     # Escape from ' as '' in sqlite-style, otherwise it's an exception.
            if isinstance(val, float):
                if is_julian(val):
                    val = from_julian(val)
            if isinstance(val, bytes):
                val = codecs.decode(val, encoding='UTF-8')
                
            data_list.append(val)
        
        try:
            cursor.execute(sql, tuple(data_list))
            conn.commit()
            rows_inserted += 1
            
        except pymssql.exceptions.OperationalError as o_error:
            msg = codecs.decode(o_error.args[1])
            # print(msg)
            
            if o_error.args[0] == 1767:
                # (1767, b"Foreign key 'None' references invalid table 'dbo.Publisher'.DB-Lib error message 20018, severity 16:\nGeneral SQL Server error: Check messages from the SQL Server\nDB-Lib error message 20018, severity 16:\nGeneral SQL Server error: Check messages from the SQL Server\n")
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
            
            print(f'SQL: {sql} | Error: {o_error}')
            raise
        except pymssql.exceptions.ProgrammingError as p_error:
            print(f'SQL: {sql} | Error: {p_error}')
            raise
        except pymssql.exceptions.IntegrityError as i_error:
            print(f'SQL: {sql} | Error: {i_error}')
            conn.rollback()
        except AttributeError as a_error:
            print(f'SQL: {sql} | Error: {a_error}')
            conn.rollback()
            raise
        except Exception as error:
            print(f'SQL: {sql} | Error: {error}')
            conn.rollback()
            raise
                    
    return rows_inserted

def insert_many(conn, table, rows, skip_primary_key=False) -> int:
    
    cursor = conn.cursor()
    columns = get_columns(conn, table)
    row_length = len(rows)
    col_length = len(columns)
    pk = None
    
    if skip_primary_key:
        pk = get_primary_key(conn, table)
    
    for row in rows:
        sql = f"INSERT INTO [{table}] "
        
        sql += "("
        for i in range(col_length):
            sql += f"[{columns[i].COLUMN_NAME}]"
            if i < col_length - 1:
                sql += ", "
            else:
                sql += ") "

    sql += "VALUES"
    for i in range(row_length):
        sql += "("
        for k in range(col_length):
            if skip_primary_key and pk and columns[k].COLUMN_NAME == pk:
                sql += "NULL"
            else:
                sql += "%s"
                
            if k < col_length - 1:
                sql += ", "
            else:
                sql += ")"
                
        if i < row_length - 1:
            sql += ","
        else:
            sql += ";"

    data_dict = {}
    for row in rows:
        temp_dict = {}
        
        for k in range(col_length):
            if skip_primary_key and pk and columns[k].COLUMN_NAME == pk:
                continue
            
            val = row[k]
            if isinstance(val, str):
                if '\'' in val:
                    val = val.replace('\'', '\'\'')     # Escape from ' as '' in sqlite-style, otherwise it's an exception.
            if isinstance(val, decimal.Decimal):
                if is_julian(val):
                    val = from_julian(val)
                if is_valid_dt_format(val):
                    val = datetime.strftime(val)    # Convert sqlite unsupported type decimal to text.
            
            temp_dict[columns[k].COLUMN_NAME] = val
            
        data_dict[row[0]] = temp_dict
        temp_dict = None
        
    sql_tuples = [v2 for k1, v1 in data_dict.items() for k2, v2 in v1.items()]
    try:
        cursor.execute(sql, tuple(sql_tuples))      
        conn.commit()
    
    except pymssql.OperationalError as o_error:
        print(f'SQL: {sql} | Error: {o_error}')
    except pymssql.ProgrammingError as p_error:
        print(f'SQL: {sql} | Error: {p_error}')
    except pymssql.IntegrityError as i_error:
        print(f'SQL: {sql} | Error: {i_error}')
        conn.rollback()
    except pymssql.Exception as error:
        print(f'SQL: {sql} | Error: {error}')
        conn.rollback()
    
    return len(data_dict)
