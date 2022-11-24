import pyodbc
import socket
import messages as msg
from pyodbc import Cursor

def insert_cond (c, connect_sec):
    status = 1

    try:
        # intento de conexion a la base secundaria
        con_sec = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}}; SERVER={connect_sec["server"]}; DATABASE={connect_sec["database"]}; UID={connect_sec["username"]}; PWD={connect_sec["password"]}')
    except:
        # error al conectar a la base secundaria
        status = 0
    else:

        # se inicializa el cursor y se busca la condicion de pago
        cursor_sec = con_sec.cursor()
        con = search_cond(cursor_sec, c.co_cond)

        if con is not None:
            # la condicion de pago ya esta en la base secundaria
            status = 2
        else:
            sp = f"""exec pInsertarCondicionPago @sco_cond = ?, @scond_des = ?, @idias_cred = ?, @sdis_cen = ?, @sCampo1 = ?, @sCampo2 = ?, 
                @sCampo3 = ?, @sCampo4 = ?, @sCampo5 = ?, @sCampo6 = ?, @sCampo7 = ?, @sCampo8 = ?, @sRevisado = ?, @sTrasnfe = ?, 
                @sco_sucu_in = ?, @sco_us_in = ?, @sMaquina = ?
            """
            params = (c.co_cond, c.cond_des, c.dias_cred, c.dis_cen, c.campo1, c.campo2, c.campo3, c.campo4, c.campo5, c.campo6, c.campo7, 
                c.campo8, c.revisado, c.trasnfe, c.co_sucu_in, 'SYNC', socket.gethostname())

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

def update_cond (item, connect_sec):
    status = 1

    try:
        # intento de conexion a la base secundaria
        con_sec = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}}; SERVER={connect_sec["server"]}; DATABASE={connect_sec["database"]}; UID={connect_sec["username"]}; PWD={connect_sec["password"]}')
    except:
        # error al conectar a la base secundaria
        status = 0
    else:

        # se inicializa el cursor y se busca la condicion de pago
        cursor_sec = con_sec.cursor()
        c = search_cond(cursor_sec, item.ItemID)

        if c is None:
            # la condicion de pago no esta en la base secundaria
            status = 2
        else:

            if item.NuevoValor is None:
                query = f"update saCondicionPago set {item.CampoModificado} = NULL, co_us_mo = 'SYNC' where co_cond = '{item.ItemID}'"
            else:
                if item.TipoDato == 'string' or item.TipoDato == 'bool':
                    query = f"update saCondicionPago set {item.CampoModificado} = '{item.NuevoValor}', co_us_mo = 'SYNC' where co_cond = '{item.ItemID}'"
                elif item.TipoDato == 'int' or item.TipoDato == 'decimal':
                    query = f"update saCondicionPago set {item.CampoModificado} = {item.NuevoValor}, co_us_mo = 'SYNC' where co_cond = '{item.ItemID}'"

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

def delete_cond (item, connect_sec):
    status = 1

    try:
        # intento de conexion a la base secundaria
        con_sec = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}}; SERVER={connect_sec["server"]}; DATABASE={connect_sec["database"]}; UID={connect_sec["username"]}; PWD={connect_sec["password"]}')
    except:
        # error al conectar a la base secundaria
        status = 0
    else:

        # se inicializa el cursor y se busca la condicion de pago
        cursor_sec = con_sec.cursor()
        c = search_cond(cursor_sec, item.ItemID)

        if c is None:
            # la condicion de pago no esta en la base secundaria
            status = 2
        else:

            sp = f"exec pEliminarCondicionPago @sco_condori = ?, @tsvalidador = ?, @smaquina = ?, @sco_us_mo = ?, @sco_sucu_mo = ?, @growguid = ?"
            params = (c.co_cond, c.validador, socket.gethostname(), 'SYNC', c.co_sucu_mo, c.rowguid)

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

def search_cond (cursor: Cursor, id):
    cursor.execute(f"select * from saCondicionPago where co_cond = '{id}'")
    c = cursor.fetchone()

    return c