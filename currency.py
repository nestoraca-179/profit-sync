import pyodbc
import socket
import messages as msg
from pyodbc import Cursor

def insert_currency (c, connect_sec):
    status = 1

    try:
        # intento de conexion a la base secundaria
        con_sec = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}}; SERVER={connect_sec["server"]}; DATABASE={connect_sec["database"]}; UID={connect_sec["username"]}; PWD={connect_sec["password"]}')
    except:
        # error al conectar a la base secundaria
        status = 0
    else:

        # se inicializa el cursor y se busca la moneda
        cursor_sec = con_sec.cursor()
        mon = search_currency(cursor_sec, c.co_mone)

        if mon is not None:
            # la moneda ya esta en la base secundaria
            status = 2
        else:
            sp = f"""exec pInsertarMoneda @sCo_Mone = ?, @sMone_Des = ?, @deCambio = ?, @bRelacion = ?, @sCampo1 = ?, @sCampo2 = ?, @sCampo3 = ?, @sCampo4 = ?, 
                @sCampo5 = ?, @sCampo6 = ?, @sCampo7 = ?, @sCampo8 = ?, @sRevisado = ?, @sTrasnfe = ?, @sco_sucu_in = ?, @sco_us_in = ?, @sMaquina = ?
            """
            params = (c.co_mone, c.mone_des, c.cambio, c.relacion, c.campo1, c.campo2, c.campo3, c.campo4, c.campo5, c.campo6, c.campo7, c.campo8, 
                c.revisado, c.trasnfe, c.co_sucu_in, 'SYNC', socket.gethostname())

            try:
                # ejecucion de script
                cursor_sec.execute(sp, params)
                cursor_sec.commit()
            except pyodbc.Error as error:
                # error en la ejecucion
                msg.print_error_msg(error)
                status = 3
                pass

        # cerrando cursor y conexion
        cursor_sec.close()
        con_sec.close()

    return status

def update_currency (item, connect_sec):
    status = 1

    try:
        # intento de conexion a la base secundaria
        con_sec = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}}; SERVER={connect_sec["server"]}; DATABASE={connect_sec["database"]}; UID={connect_sec["username"]}; PWD={connect_sec["password"]}')
    except:
        # error al conectar a la base secundaria
        status = 0
    else:

        # se inicializa el cursor y se busca la moneda
        cursor_sec = con_sec.cursor()
        c = search_currency(cursor_sec, item.ItemID)

        if c is None:
            # la moneda no esta en la base secundaria
            status = 2
        else:

            if item.NuevoValor is None:
                query = f"update saMoneda set {item.CampoModificado} = NULL, co_us_mo = 'SYNC' where co_mone = '{item.ItemID}'"
            else:
                if item.TipoDato == 'string' or item.TipoDato == 'bool':
                    query = f"update saMoneda set {item.CampoModificado} = '{item.NuevoValor}', co_us_mo = 'SYNC' where co_mone = '{item.ItemID}'"
                elif item.TipoDato == 'int' or item.TipoDato == 'decimal':
                    query = f"update saMoneda set {item.CampoModificado} = {item.NuevoValor}, co_us_mo = 'SYNC' where co_mone = '{item.ItemID}'"

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

def delete_currency (item, connect_sec):
    status = 1

    try:
        # intento de conexion a la base secundaria
        con_sec = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}}; SERVER={connect_sec["server"]}; DATABASE={connect_sec["database"]}; UID={connect_sec["username"]}; PWD={connect_sec["password"]}')
    except:
        # error al conectar a la base secundaria
        status = 0
    else:

        # se inicializa el cursor y se busca la moneda
        cursor_sec = con_sec.cursor()
        c = search_currency(cursor_sec, item.ItemID)

        if c is None:
            # la moneda no esta en la base secundaria
            status = 2
        else:

            sp = f"exec pEliminarMoneda @sco_moneori = ?, @tsvalidador = ?, @smaquina = ?, @sco_us_mo = ?, @sco_sucu_mo = ?, @growguid = ?"
            params = (c.co_mone, c.validador, socket.gethostname(), 'SYNC', None, c.rowguid)

            try:
                # ejecucion de script
                cursor_sec.execute(sp, params)
                cursor_sec.commit()
            except pyodbc.Error as error:
                # error en la ejecucion
                msg.print_error_msg(error)
                status = 3
                pass

        cursor_sec.close()
        con_sec.close()

    return status

def search_currency (cursor: Cursor, id):
    cursor.execute(f"select * from saMoneda where co_mone = '{id}'")
    c = cursor.fetchone()

    return c