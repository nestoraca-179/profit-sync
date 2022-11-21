import pyodbc
import messages as msg
from pyodbc import Cursor

def update_buy_doc(item, code, connect_sec):
    status = 1

    try:
        # intento de conexion a la base secundaria
        con_sec = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}}; SERVER={connect_sec["server"]}; DATABASE={connect_sec["database"]}; UID={connect_sec["username"]}; PWD={connect_sec["password"]}')
    except:
        # error al conectar a la base secundaria
        status = 0
    else:

        # se inicializa el cursor y se busca el documento
        cursor_sec = con_sec.cursor()
        d = search_buy_doc(cursor_sec, code, item.ItemID)

        if d is None:
            # el documento no esta en la base secundaria
            status = 2
        else:

            if item.NuevoValor is None:
                query = f"""update saDocumentoCompra set {item.CampoModificado} = NULL, co_us_mo = 'SYNC'
                            where co_tipo_doc = '{code}' and nro_doc = '{item.ItemID}'"""
            else:
                if item.TipoDato == 'string' or item.TipoDato == 'bool':
                    query = f"""update saDocumentoCompra set {item.CampoModificado} = '{item.NuevoValor}', co_us_mo = 'SYNC'
                                where co_tipo_doc = '{code}' and nro_doc = '{item.ItemID}'"""
                elif item.TipoDato == 'int' or item.TipoDato == 'decimal':
                    query = f"""update saDocumentoCompra set {item.CampoModificado} = {item.NuevoValor}, co_us_mo = 'SYNC'
                                where co_tipo_doc = '{code}' and nro_doc = '{item.ItemID}'"""

            try:
                # ejecucion de script
                cursor_sec.execute(query)
                cursor_sec.commit()
            except pyodbc.Error as error:
                # error en la ejecucion
                msg.print_error_msg(error)
                status = 3
                pass

        cursor_sec.close()
        con_sec.close()
    
    return status

def search_buy_doc(cursor: Cursor, code, id):
    cursor.execute(f"select * from saDocumentoCompra where co_tipo_doc = '{code}' and nro_doc = '{id}'")
    doc = cursor.fetchone()

    return doc