import pyodbc
import socket
import messages as msg
from pyodbc import Cursor

def insert_zone (z, connect_sec):
    status = 1

    try:
        # intento de conexion a la base secundaria
        con_sec = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}}; SERVER={connect_sec["server"]}; DATABASE={connect_sec["database"]}; UID={connect_sec["username"]}; PWD={connect_sec["password"]}')
    except:
        # error al conectar a la base secundaria
        status = 0
    else:

        # se inicializa el cursor y se busca la zona
        cursor_sec = con_sec.cursor()
        zon = search_zone(cursor_sec, z.co_zon)

        if zon is not None:
            # la zona ya esta en la base secundaria
            status = 2
        else:
            sp = f"""exec pInsertarZona @sCo_Zon = ?, @sZon_Des = ?, @sdis_cen = ?, @sCampo1 = ?, @sCampo2 = ?, @sCampo3 = ?, @sCampo4 = ?, 
                @sCampo5 = ?, @sCampo6 = ?, @sCampo7 = ?, @sCampo8 = ?, @sRevisado = ?, @sTrasnfe = ?, @sco_sucu_in = ?, @sco_us_in = ?, @sMaquina = ?
            """
            params = (z.co_zon, z.zon_des, z.dis_cen, z.campo1, z.campo2, z.campo3, z.campo4, z.campo5, z.campo6, z.campo7, 
                z.campo8, z.revisado, z.trasnfe, z.co_sucu_in, 'SYNC', socket.gethostname())

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

def update_zone (item, connect_sec):
    status = 1

    try:
        # intento de conexion a la base secundaria
        con_sec = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}}; SERVER={connect_sec["server"]}; DATABASE={connect_sec["database"]}; UID={connect_sec["username"]}; PWD={connect_sec["password"]}')
    except:
        # error al conectar a la base secundaria
        status = 0
    else:

        # se inicializa el cursor y se busca la zona
        cursor_sec = con_sec.cursor()
        z = search_zone(cursor_sec, item.ItemID)

        if z is None:
            # la zona no esta en la base secundaria
            status = 2
        else:

            if item.NuevoValor is None:
                query = f"update saZona set {item.CampoModificado} = NULL, co_us_mo = 'SYNC' where co_zon = '{item.ItemID}'"
            else:
                if item.TipoDato == 'string' or item.TipoDato == 'bool':
                    query = f"update saZona set {item.CampoModificado} = '{item.NuevoValor}', co_us_mo = 'SYNC' where co_zon = '{item.ItemID}'"
                elif item.TipoDato == 'int' or item.TipoDato == 'decimal':
                    query = f"update saZona set {item.CampoModificado} = {item.NuevoValor}, co_us_mo = 'SYNC' where co_zon = '{item.ItemID}'"

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

def delete_zone (item, connect_sec):
    status = 1

    try:
        # intento de conexion a la base secundaria
        con_sec = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}}; SERVER={connect_sec["server"]}; DATABASE={connect_sec["database"]}; UID={connect_sec["username"]}; PWD={connect_sec["password"]}')
    except:
        # error al conectar a la base secundaria
        status = 0
    else:

        # se inicializa el cursor y se busca la zona
        cursor_sec = con_sec.cursor()
        z = search_zone(cursor_sec, item.ItemID)

        if z is None:
            # la zona no esta en la base secundaria
            status = 2
        else:

            sp = f"exec pEliminarZona @sco_zonori = ?, @tsvalidador = ?, @smaquina = ?, @sco_us_mo = ?, @sco_sucu_mo = ?, @growguid = ?"
            params = (z.co_zon, z.validador, socket.gethostname(), 'SYNC', z.co_sucu_mo, z.rowguid)

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

def search_zone (cursor: Cursor, id):
    cursor.execute(f"select * from saZona where co_zon = '{id}'")
    c = cursor.fetchone()

    return c