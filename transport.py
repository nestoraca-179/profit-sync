import pyodbc
import socket
import messages as msg
from pyodbc import Cursor

def insert_transport (t, connect_sec):
    status = 1

    try:
        # intento de conexion a la base secundaria
        con_sec = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}}; SERVER={connect_sec["server"]}; DATABASE={connect_sec["database"]}; UID={connect_sec["username"]}; PWD={connect_sec["password"]}')
    except:
        # error al conectar a la base secundaria
        status = 0
    else:

        # se inicializa el cursor y se busca el transporte
        cursor_sec = con_sec.cursor()
        sel = search_transport(cursor_sec, t.co_tran)

        if sel is not None:
            # el transporte ya esta en la base secundaria
            status = 2
        else:
            sp = f"""exec pInsertarTransporte @sCo_Tran = ?, @sdes_tran = ?, @sresp_tra = ?, @sdis_cen = ?, @sCampo1 = ?, @sCampo2 = ?, 
                @sCampo3 = ?, @sCampo4 = ?, @sCampo5 = ?, @sCampo6 = ?, @sCampo7 = ?, @sCampo8 = ?, @sRevisado = ?, @sTrasnfe = ?, 
                @sco_sucu_in = ?, @sco_us_in = ?, @sMaquina = ?
            """
            params = (t.co_tran, t.des_tran, t.resp_tra, t.dis_cen, t.campo1, t.campo2, t.campo3, t.campo4, t.campo5, t.campo6, t.campo7, 
                t.campo8, t.revisado, t.trasnfe, t.co_sucu_in, 'SYNC', socket.gethostname())

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

def update_transport (item, connect_sec):
    status = 1

    try:
        # intento de conexion a la base secundaria
        con_sec = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}}; SERVER={connect_sec["server"]}; DATABASE={connect_sec["database"]}; UID={connect_sec["username"]}; PWD={connect_sec["password"]}')
    except:
        # error al conectar a la base secundaria
        status = 0
    else:

        # se inicializa el cursor y se busca el transporte
        cursor_sec = con_sec.cursor()
        t = search_transport(cursor_sec, item.ItemID)

        if t is None:
            # el transporte no esta en la base secundaria
            status = 2
        else:

            if item.NuevoValor is None:
                query = f"update saTransporte set {item.CampoModificado} = NULL, co_us_mo = 'SYNC' where co_tran = '{item.ItemID}'"
            else:
                if item.TipoDato == 'string' or item.TipoDato == 'bool':
                    query = f"update saTransporte set {item.CampoModificado} = '{item.NuevoValor}', co_us_mo = 'SYNC' where co_tran = '{item.ItemID}'"
                elif item.TipoDato == 'int' or item.TipoDato == 'decimal':
                    query = f"update saTransporte set {item.CampoModificado} = {item.NuevoValor}, co_us_mo = 'SYNC' where co_tran = '{item.ItemID}'"

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

def delete_transport (item, connect_sec):
    status = 1

    try:
        # intento de conexion a la base secundaria
        con_sec = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}}; SERVER={connect_sec["server"]}; DATABASE={connect_sec["database"]}; UID={connect_sec["username"]}; PWD={connect_sec["password"]}')
    except:
        # error al conectar a la base secundaria
        status = 0
    else:

        # se inicializa el cursor y se busca el transporte
        cursor_sec = con_sec.cursor()
        t = search_transport(cursor_sec, item.ItemID)

        if t is None:
            # el transporte no esta en la base secundaria
            status = 2
        else:

            sp = f"exec pEliminarTransporte @sco_tranori = ?, @tsvalidador = ?, @smaquina = ?, @sco_us_mo = ?, @sco_sucu_mo = ?, @growguid = ?"
            params = (t.co_tran, t.validador, socket.gethostname(), 'SYNC', t.co_sucu_mo, t.rowguid)

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

def search_transport (cursor: Cursor, id):
    cursor.execute(f"select * from saTransporte where co_tran = '{id}'")
    c = cursor.fetchone()

    return c