import pyodbc
import messages as msg
from datetime import datetime

connect_sync = {
    "server": "IT-MOV-91\SQLS2014SE",
    "database": "ProfitSync",
    "username": "sa",
    "password": "Soporte123456"
}

class SyncManager:

    def __init__(self):
        self.con_sync = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}}; SERVER={connect_sync["server"]}; DATABASE={connect_sync["database"]}; UID={connect_sync["username"]}; PWD={connect_sync["password"]}')
        self.cursor = self.con_sync.cursor()

    def get_items_delete(self):
        self.cursor.execute('select * from ItemsEliminar where Sincronizado = 0 order by Fecha')
        return self.cursor.fetchall()

    def get_items_update(self):
        self.cursor.execute('select * from VIEW_ITEMS_MODIFICAR where Sincronizado = 0 order by Fecha')
        return self.cursor.fetchall()

    def get_items_insert(self):
        self.cursor.execute('select * from ItemsAgregar where Sincronizado = 0 order by Fecha')
        return self.cursor.fetchall()

    def update_item (self, table, id):
        now = datetime.now()

        self.cursor.execute(f"update {table} set Sincronizado = 1, FechaSincronizado = '{now.strftime('%m/%d/%Y %H:%M:%S')}' where ID = {id}")
        self.cursor.commit()

    def delete_item (self, table, id):
        self.cursor.execute(f'delete from {table} where ID = {id}')
        self.cursor.commit()

    def __del__(self):
        self.cursor.close()
        self.con_sync.close()
        msg.print_end_sync()