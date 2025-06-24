import datetime
import decimal
from sqlite3 import OperationalError, ProgrammingError, IntegrityError

from bdatetime.bdatetime import to_julian
from sql.sqlite.schemaimpl import get_columns, get_primary_key

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
    except OperationalError as op_error:
        print(op_error)
    except Exception as error:
        print(error)
        
    return rows
    
def insert_one_by_one(conn, table, rows, skip_primary_key=True) -> int:

    cursor = conn.cursor()
    rows_inserted = 0
    columns = get_columns(conn, table)
    length = len(columns)
    pk = None
    
    if skip_primary_key:
        pk = get_primary_key(conn, table)
    
    for row in rows:
        sql = f"INSERT INTO [{table}] "
        
        sql += "("
        for i in range(length):
            sql += f"[{columns[i].COLUMN_NAME}]"
            if i < length - 1:
                sql += ", "
            else:
                sql += ") "
        
        sql += "VALUES("
        for i in range(length):
            if skip_primary_key and pk and columns[i].COLUMN_NAME == pk:
                sql += "NULL"
            else:
                sql += "?"
                
            if i < length - 1:
                sql += ", "
            else:
                sql += ")"
        
        data_list = []
        for k in range(len(row)):
            if skip_primary_key and pk and columns[k].COLUMN_NAME == pk:
                continue
            
            val = row[k]
            if isinstance(val, datetime.datetime):
                val = to_julian(val)
            if isinstance(val, str) and '\'' in val:
                val = val.replace('\'', '\'\'')     # Escape from ' as '' in sqlite-style, otherwise it's an exception.
            if isinstance(val, decimal.Decimal):
                val = str(val)          # Convert sqlite unsupported type decimal to text.
                
            data_list.append(val)
        
        try:
            cursor.execute(sql, tuple(data_list))       # tuple(row) doesn't work because of unsupported types between sqlserver and sqlite. e.g. type decimal.
            conn.commit()
            rows_inserted += 1
        
        except OperationalError as o_error:
            print(f'SQL: {sql} | Error: {o_error}')
        except ProgrammingError as p_error:
            print(f'SQL: {sql} | Error: {p_error}')
        except IntegrityError as i_error:
            print(f'SQL: {sql} | Error: {i_error}')
            conn.rollback()
        except Exception as error:
            print(f'SQL: {sql} | Error: {error}')
            conn.rollback()
            
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
    for i in range(row_length):         # Create a query string with multiple lines of values
        sql += "("
        for k in range(col_length):
            if skip_primary_key and pk and columns[k].COLUMN_NAME == pk:
                sql += "NULL"
            else:
                sql += "?"
                
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
            if isinstance(val, datetime.datetime):
                val = to_julian(val)
            if isinstance(val, str) and '\'' in val:
                val = val.replace('\'', '\'\'')     # Escape from ' as '' in sqlite-style, otherwise it's an exception.
            if isinstance(val, decimal.Decimal):
                val = str(val)                      # Convert sqlite unsupported type decimal to text.
            
            temp_dict[columns[k].COLUMN_NAME] = val
            
        data_dict[row[0]] = temp_dict
        temp_dict = None
        
    sql_tuples = [v2 for k1, v1 in data_dict.items() for k2, v2 in v1.items()]
    try:
        cursor.execute(sql, tuple(sql_tuples))      
        conn.commit()
    
    except OperationalError as o_error:
        print(f'SQL: {sql} | Error: {o_error}')
    except ProgrammingError as p_error:
        print(f'SQL: {sql} | Error: {p_error}')
    except IntegrityError as i_error:
        print(f'SQL: {sql} | Error: {i_error}')
        conn.rollback()
    except Exception as error:
        print(f'SQL: {sql} | Error: {error}')
        conn.rollback()
    
    return len(data_dict)
