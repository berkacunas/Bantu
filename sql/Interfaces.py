from collections import namedtuple

Column = namedtuple('Column', ['TABLE_NAME', 'COLUMN_NAME', 'ORDINAL_POSITION', 'IS_NULLABLE', 'DATA_TYPE', 'CHARACTER_MAXIMUM_LENGTH', 'DATETIME_PRECISION', 'IS_PK', 'DEFAULT_VALUE'])
Foreign_Key = namedtuple('Foreign_Key', ['ID', 'SEQ', 'REFERENCING_TABLE_NAME', 'REFERENCED_TABLE_NAME', 'REFERENCING_COLUMN_NAME', 'REFERENCED_COLUMN_NAME', 'CONSTRAINT_NAME', 'ON_UPDATE', 'ON_DELETE', 'MATCH'])

sqlite_to_sqlserver_types_dict = {
    "TEXT": ["varchar", "nvarchar", "char", "nchar", "text", "ntext"],
    "INTEGER": ["tinyint", "smallint", "int", "bigint", "bit", ""],
    "REAL": ["date", "time", "datetime", "datetime2", "datetimeoffset", "smalldatetime", "float", "real"],
    "NUMERIC": ["decimal", "numeric", "money", "smallmoney"],
    "BLOB": ["binary", "varbinary", "image"],
    "OTHER": ["cursor", "geography", "geometry", "hierarchyid", "json", "vector", "rowversion", "sql_variant", "table", "uniqueidentifier", "xml"]
}

sqlserver_to_sqlite_types_dict = {
    "TEXT": ["nvarchar(max)"],
    "INTEGER": ["int"],
    "REAL": ["datetime2(7)"],       # , "real"
    "NUMERIC": ["decimal(18,2)"],   # , "numeric(18,2)"
    "BLOB": ["varbinary(max)"]      # , "image"
}