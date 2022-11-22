import pyodbc
import socket
import messages as msg
from pyodbc import Cursor

def insert_account (a, connect_sec):
    status = 1

    try:
        # intento de conexion a la base secundaria
        con_sec = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}}; SERVER={connect_sec["server"]}; DATABASE={connect_sec["database"]}; UID={connect_sec["username"]}; PWD={connect_sec["password"]}')
    except:
        # error al conectar a la base secundaria
        status = 0
    else:

        # se inicializa el cursor y se busca la cuenta
        cursor_sec = con_sec.cursor()
        acc = search_account(cursor_sec, a.co_cta_ingr_egr)

        if acc is not None:
            # la cuenta ya esta en la base secundaria
            status = 2
        else:
            sp = f"""exec pInsertarCuentaIngreso @sco_cta_ingr_egr = ?, @sDescrip = ?, @sCo_Islr = ?, @sDis_Cen = ?, @sCampo1 = ?, @sCampo2 = ?, 
                @sCampo3 = ?, @sCampo4 = ?, @sCampo5 = ?, @sCampo6 = ?, @sCampo7 = ?, @sCampo8 = ?, @sRevisado = ?, @sTrasnfe = ?, @sco_sucu_in = ?, 
                @sco_us_in = ?, @sMaquina = ?
            """
            params = (a.co_cta_ingr_egr, a.descrip, a.co_islr, a.dis_cen, a.campo1, a.campo2, a.campo3, a.campo4, a.campo5, a.campo6, a.campo7, 
                a.campo8, a.revisado, a.trasnfe, a.co_sucu_in, 'SYNC', socket.gethostname())

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

def update_account (item, connect_sec):
    status = 1

    try:
        # intento de conexion a la base secundaria
        con_sec = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}}; SERVER={connect_sec["server"]}; DATABASE={connect_sec["database"]}; UID={connect_sec["username"]}; PWD={connect_sec["password"]}')
    except:
        # error al conectar a la base secundaria
        status = 0
    else:

        # se inicializa el cursor y se busca la cuenta
        cursor_sec = con_sec.cursor()
        a = search_account(cursor_sec, item.ItemID)

        if a is None:
            # la cuenta no esta en la base secundaria
            status = 2
        else:

            if item.NuevoValor is None:
                query = f"update saCuentaIngEgr set {item.CampoModificado} = NULL, co_us_mo = 'SYNC' where co_cta_ingr_egr = '{item.ItemID}'"
            else:
                if item.TipoDato == 'string' or item.TipoDato == 'bool':
                    query = f"update saCuentaIngEgr set {item.CampoModificado} = '{item.NuevoValor}', co_us_mo = 'SYNC' where co_cta_ingr_egr = '{item.ItemID}'"
                elif item.TipoDato == 'int' or item.TipoDato == 'decimal':
                    query = f"update saCuentaIngEgr set {item.CampoModificado} = {item.NuevoValor}, co_us_mo = 'SYNC' where co_cta_ingr_egr = '{item.ItemID}'"

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

def delete_account (item, connect_sec):
    status = 1

    try:
        # intento de conexion a la base secundaria
        con_sec = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}}; SERVER={connect_sec["server"]}; DATABASE={connect_sec["database"]}; UID={connect_sec["username"]}; PWD={connect_sec["password"]}')
    except:
        # error al conectar a la base secundaria
        status = 0
    else:

        # se inicializa el cursor y se busca la cuenta
        cursor_sec = con_sec.cursor()
        a = search_account(cursor_sec, item.ItemID)

        if a is None:
            # la cuenta no esta en la base secundaria
            status = 2
        else:

            sp = f"exec pEliminarCuentaIngreso @sco_cta_ingr_egrori = ?, @tsvalidador = ?, @smaquina = ?, @sco_us_mo = ?, @sco_sucu_mo = ?, @growguid = ?"
            params = (a.co_cta_ingr_egr, a.validador, socket.gethostname(), 'SYNC', a.co_sucu_mo, a.rowguid)

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

def search_account (cursor: Cursor, id):
    cursor.execute(f"select * from saCuentaIngEgr where co_cta_ingr_egr = '{id}'")
    c = cursor.fetchone()

    return c