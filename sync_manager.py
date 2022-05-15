from pyodbc import Cursor
from datetime import datetime

def update_item (cursor: Cursor, table, id):

    now = datetime.now()

    cursor.execute(f"update {table} set Sincronizado = 1, FechaSincronizado = '{now.strftime('%m/%d/%Y %H:%M:%S')}' where ID = {id}")
    cursor.commit()

def delete_item (cursor: Cursor, table, id):
    
    cursor.execute(f'delete from {table} where ID = {id}')
    cursor.commit()