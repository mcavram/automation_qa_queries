import sqlalchemy as sql

def connect_sql(server_address, username, password, database_name='matrix', driver='driver=ODBC+Driver+17+for+SQL+Server'):
    return sql.create_engine('mssql+pyodbc://{}:{}@{}/{}?{}'.format(username, password, server_address, database_name, driver))
