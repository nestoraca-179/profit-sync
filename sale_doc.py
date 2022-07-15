from pyodbc import Cursor
import pyodbc
import messages as msg

def update_sale_doc(item, connect_sec):
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
        d = search_sale_doc(cursor_sec, 'FACT', item.ItemID)

        if d is None:
            # el documento no esta en la base secundaria
            status = 2
        else:

            if item.NuevoValor is None:
                query = f"""update saDocumentoVenta set {item.CampoModificado} = NULL
                            where co_tipo_doc = 'FACT' and nro_doc = '{item.ItemID}'"""
            else:
                if item.TipoDato == 'string' or item.TipoDato == 'bool':
                    query = f"""update saDocumentoVenta set {item.CampoModificado} = '{item.NuevoValor}'
                                where co_tipo_doc = 'FACT' and nro_doc = '{item.ItemID}'"""
                elif item.TipoDato == 'int' or item.TipoDato == 'decimal':
                    query = f"""update saDocumentoVenta set {item.CampoModificado} = {item.NuevoValor}
                                where co_tipo_doc = 'FACT' and nro_doc = '{item.ItemID}'"""

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

def search_sale_doc(cursor: Cursor, code, id):
    cursor.execute(f"select * from saDocumentoVenta where co_tipo_doc = '{code}' and nro_doc = '{id}'")
    doc = cursor.fetchone()

    return doc