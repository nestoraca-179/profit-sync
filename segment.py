import pyodbc
import socket
import messages as msg
from pyodbc import Cursor

def insert_segment (s, connect_sec):
    status = 1

    try:
        # intento de conexion a la base secundaria
        con_sec = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}}; SERVER={connect_sec["server"]}; DATABASE={connect_sec["database"]}; UID={connect_sec["username"]}; PWD={connect_sec["password"]}')
    except:
        # error al conectar a la base secundaria
        status = 0
    else:

        # se inicializa el cursor y se busca el segmento
        cursor_sec = con_sec.cursor()
        seg = search_segment(cursor_sec, s.co_seg)

        if seg is not None:
            # el segmento ya esta en la base secundaria
            status = 2
        else:
            sp = f"""exec pInsertarSegmento @sCo_Seg = ?, @sseg_des = ?, @sc_cuenta = ?, @sp_cuenta = ?, @sdis_cen = ?, @sCampo1 = ?, @sCampo2 = ?, 
                @sCampo3 = ?, @sCampo4 = ?, @sCampo5 = ?, @sCampo6 = ?, @sCampo7 = ?, @sCampo8 = ?, @sRevisado = ?, @sTrasnfe = ?, 
                @sco_sucu_in = ?, @sco_us_in = ?, @sMaquina = ?
            """
            params = (s.co_seg, s.seg_des, s.c_cuenta, s.p_cuenta, s.dis_cen, s.campo1, s.campo2, s.campo3, s.campo4, s.campo5, s.campo6, 
                s.campo7, s.campo8, s.revisado, s.trasnfe, s.co_sucu_in, 'SYNC', socket.gethostname())

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

def update_segment (item, connect_sec):
    status = 1

    try:
        # intento de conexion a la base secundaria
        con_sec = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}}; SERVER={connect_sec["server"]}; DATABASE={connect_sec["database"]}; UID={connect_sec["username"]}; PWD={connect_sec["password"]}')
    except:
        # error al conectar a la base secundaria
        status = 0
    else:

        # se inicializa el cursor y se busca el segmento
        cursor_sec = con_sec.cursor()
        s = search_segment(cursor_sec, item.ItemID)

        if s is None:
            # el segmento no esta en la base secundaria
            status = 2
        else:

            if item.NuevoValor is None:
                query = f"update saSegmento set {item.CampoModificado} = NULL, co_us_mo = 'SYNC' where co_seg = '{item.ItemID}'"
            else:
                if item.TipoDato == 'string' or item.TipoDato == 'bool':
                    query = f"update saSegmento set {item.CampoModificado} = '{item.NuevoValor}', co_us_mo = 'SYNC' where co_seg = '{item.ItemID}'"
                elif item.TipoDato == 'int' or item.TipoDato == 'decimal':
                    query = f"update saSegmento set {item.CampoModificado} = {item.NuevoValor}, co_us_mo = 'SYNC' where co_seg = '{item.ItemID}'"

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

def delete_segment (item, connect_sec):
    status = 1

    try:
        # intento de conexion a la base secundaria
        con_sec = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}}; SERVER={connect_sec["server"]}; DATABASE={connect_sec["database"]}; UID={connect_sec["username"]}; PWD={connect_sec["password"]}')
    except:
        # error al conectar a la base secundaria
        status = 0
    else:

        # se inicializa el cursor y se busca el segmento
        cursor_sec = con_sec.cursor()
        s = search_segment(cursor_sec, item.ItemID)

        if s is None:
            # el segmento no esta en la base secundaria
            status = 2
        else:

            sp = f"exec pEliminarSegmento @sco_segori = ?, @tsvalidador = ?, @smaquina = ?, @sco_us_mo = ?, @sco_sucu_mo = ?, @growguid = ?"
            params = (s.co_seg, s.validador, socket.gethostname(), 'SYNC', s.co_sucu_mo, s.rowguid)

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

def search_segment (cursor: Cursor, id):
    cursor.execute(f"select * from saSegmento where co_seg = '{id}'")
    c = cursor.fetchone()

    return c