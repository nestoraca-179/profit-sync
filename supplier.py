import pyodbc
from pyodbc import Cursor

def search_supplier(cursor: Cursor, id):
    cursor.execute(f"select * from saProveedor where co_prov = '{id}'")
    s = cursor.fetchone()

    return s