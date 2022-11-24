import pyodbc
import socket
import messages as msg
from pyodbc import Cursor

def insert_seller (s, connect_sec):
    status = 1

    try:
        # intento de conexion a la base secundaria
        con_sec = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}}; SERVER={connect_sec["server"]}; DATABASE={connect_sec["database"]}; UID={connect_sec["username"]}; PWD={connect_sec["password"]}')
    except:
        # error al conectar a la base secundaria
        status = 0
    else:

        # se inicializa el cursor y se busca el vendedor
        cursor_sec = con_sec.cursor()
        sel = search_seller(cursor_sec, s.co_ven)

        if sel is not None:
            # el vendedor ya esta en la base secundaria
            status = 2
        else:
            sp = f"""exec pInsertarVendedor @sCo_Ven = ?, @sVen_Des = ?, @sTipo = ?, @sDis_Cen = ?, @sCedula = ?, @sDirec1 = ?, @sDirec2 = ?, 
                @sTelefonos = ?, @sdfecha_reg = ?, @bInactivo = ?, @deComision = ?, @sComentario = ?, @bFun_Cob = ?, @bFun_Ven = ?, @deComisionV = ?, 
                @sLogin = ?, @sPassword = ?, @sEmail = ?, @sPSW_M = ?, @sco_zon = ?, @sCampo1 = ?, @sCampo2 = ?, @sCampo3 = ?, @sCampo4 = ?, 
                @sCampo5 = ?, @sCampo6 = ?, @sCampo7 = ?, @sCampo8 = ?, @sRevisado = ?, @sTrasnfe = ?, @sco_sucu_in = ?, @sco_us_in = ?, @sMaquina = ?
            """
            params = (s.co_ven, s.ven_des, s.tipo, s.dis_cen, s.cedula, s.direc1, s.direc2, s.telefonos, s.fecha_reg, s.inactivo, s.comision, 
                s.comentario, s.fun_cob, s.fun_ven, s.comisionv, s.login, s.password, s.email, s.PSW_M, s.co_zon, s.campo1, s.campo2, s.campo3, 
                s.campo4, s.campo5, s.campo6, s.campo7, s.campo8, s.revisado, s.trasnfe, s.co_sucu_in, 'SYNC', socket.gethostname())

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

def update_seller (item, connect_sec):
    status = 1

    try:
        # intento de conexion a la base secundaria
        con_sec = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}}; SERVER={connect_sec["server"]}; DATABASE={connect_sec["database"]}; UID={connect_sec["username"]}; PWD={connect_sec["password"]}')
    except:
        # error al conectar a la base secundaria
        status = 0
    else:

        # se inicializa el cursor y se busca el vendedor
        cursor_sec = con_sec.cursor()
        s = search_seller(cursor_sec, item.ItemID)

        if s is None:
            # el vendedor no esta en la base secundaria
            status = 2
        else:

            if item.NuevoValor is None:
                query = f"update saVendedor set {item.CampoModificado} = NULL, co_us_mo = 'SYNC' where co_ven = '{item.ItemID}'"
            else:
                if item.TipoDato == 'string' or item.TipoDato == 'bool':
                    query = f"update saVendedor set {item.CampoModificado} = '{item.NuevoValor}', co_us_mo = 'SYNC' where co_ven = '{item.ItemID}'"
                elif item.TipoDato == 'int' or item.TipoDato == 'decimal':
                    query = f"update saVendedor set {item.CampoModificado} = {item.NuevoValor}, co_us_mo = 'SYNC' where co_ven = '{item.ItemID}'"

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

def delete_seller (item, connect_sec):
    status = 1

    try:
        # intento de conexion a la base secundaria
        con_sec = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}}; SERVER={connect_sec["server"]}; DATABASE={connect_sec["database"]}; UID={connect_sec["username"]}; PWD={connect_sec["password"]}')
    except:
        # error al conectar a la base secundaria
        status = 0
    else:

        # se inicializa el cursor y se busca el vendedor
        cursor_sec = con_sec.cursor()
        s = search_seller(cursor_sec, item.ItemID)

        if s is None:
            # el vendedor no esta en la base secundaria
            status = 2
        else:

            sp = f"exec pEliminarVendedor @sco_venori = ?, @tsvalidador = ?, @smaquina = ?, @sco_us_mo = ?, @sco_sucu_mo = ?, @growguid = ?"
            params = (s.co_ven, s.validador, socket.gethostname(), 'SYNC', s.co_sucu_mo, s.rowguid)

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

def search_seller (cursor: Cursor, id):
    cursor.execute(f"select * from saVendedor where co_ven = '{id}'")
    c = cursor.fetchone()

    return c