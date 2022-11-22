import pyodbc
import socket
import messages as msg
from pyodbc import Cursor

def insert_price_type (t, connect_sec):
    status = 1

    try:
        # intento de conexion a la base secundaria
        con_sec = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}}; SERVER={connect_sec["server"]}; DATABASE={connect_sec["database"]}; UID={connect_sec["username"]}; PWD={connect_sec["password"]}')
    except:
        # error al conectar a la base secundaria
        status = 0
    else:

        # se inicializa el cursor y se busca el tipo de precio
        cursor_sec = con_sec.cursor()
        type = search_price_type(cursor_sec, t.co_precio)

        if type is not None:
            # el tipo de precio ya esta en la base secundaria
            status = 2
        else:
            sp = f"""exec pInsertarTipoPrecio @sCo_Precio = ?, @sDes_Precio = ?, @bIncluye_imp = ?, @sCampo1 = ?, @sCampo2 = ?, @sCampo3 = ?, 
                @sCampo4 = ?, @sCampo5 = ?, @sCampo6 = ?, @sCampo7 = ?, @sCampo8 = ?, @sRevisado = ?, @sTrasnfe = ?, @sco_sucu_in = ?, 
                @sco_us_in = ?, @sMaquina = ?
            """
            params = (t.co_precio, t.des_precio, t.incluye_imp, t.campo1, t.campo2, t.campo3, t.campo4, t.campo5, t.campo6, t.campo7, t.campo8, 
                t.revisado, t.trasnfe, t.co_sucu_in, 'SYNC', socket.gethostname())

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

def update_price_type (item, connect_sec):
    status = 1

    try:
        # intento de conexion a la base secundaria
        con_sec = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}}; SERVER={connect_sec["server"]}; DATABASE={connect_sec["database"]}; UID={connect_sec["username"]}; PWD={connect_sec["password"]}')
    except:
        # error al conectar a la base secundaria
        status = 0
    else:

        # se inicializa el cursor y se busca el tipo de precio
        cursor_sec = con_sec.cursor()
        t = search_price_type(cursor_sec, item.ItemID)

        if t is None:
            # el tipo de precio no esta en la base secundaria
            status = 2
        else:

            if item.NuevoValor is None:
                query = f"update saTipoPrecio set {item.CampoModificado} = NULL, co_us_mo = 'SYNC' where co_precio = '{item.ItemID}'"
            else:
                if item.TipoDato == 'string' or item.TipoDato == 'bool':
                    query = f"update saTipoPrecio set {item.CampoModificado} = '{item.NuevoValor}', co_us_mo = 'SYNC' where co_precio = '{item.ItemID}'"
                elif item.TipoDato == 'int' or item.TipoDato == 'decimal':
                    query = f"update saTipoPrecio set {item.CampoModificado} = {item.NuevoValor}, co_us_mo = 'SYNC' where co_precio = '{item.ItemID}'"

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

def delete_price_type (item, connect_sec):
    status = 1

    try:
        # intento de conexion a la base secundaria
        con_sec = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}}; SERVER={connect_sec["server"]}; DATABASE={connect_sec["database"]}; UID={connect_sec["username"]}; PWD={connect_sec["password"]}')
    except:
        # error al conectar a la base secundaria
        status = 0
    else:

        # se inicializa el cursor y se busca el tipo de precio
        cursor_sec = con_sec.cursor()
        t = search_price_type(cursor_sec, item.ItemID)

        if t is None:
            # el tipo de precio no esta en la base secundaria
            status = 2
        else:

            sp = f"exec pEliminarTipoPrecio @sco_precioori = ?, @tsvalidador = ?, @smaquina = ?, @sco_us_mo = ?, @sco_sucu_mo = ?, @growguid = ?"
            params = (t.co_precio, t.validador, socket.gethostname(), 'SYNC', t.co_sucu_mo, t.rowguid)

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

def search_price_type (cursor: Cursor, id):
    cursor.execute(f"select * from saTipoPrecio where co_precio = '{id}'")
    c = cursor.fetchone()

    return c

def insert_client_type (t, connect_sec):
    status = 1

    try:
        # intento de conexion a la base secundaria
        con_sec = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}}; SERVER={connect_sec["server"]}; DATABASE={connect_sec["database"]}; UID={connect_sec["username"]}; PWD={connect_sec["password"]}')
    except:
        # error al conectar a la base secundaria
        status = 0
    else:

        # se inicializa el cursor y se busca el tipo de cliente
        cursor_sec = con_sec.cursor()
        type = search_client_type(cursor_sec, t.tip_cli)

        if type is not None:
            # el tipo de cliente ya esta en la base secundaria
            status = 2
        else:
            sp = f"""exec pInsertarTipoCliente @sTip_Cli = ?, @sdes_tipo = ?, @sco_precio = ?, @sCampo1 = ?, @sCampo2 = ?, @sCampo3 = ?, @sCampo4 = ?, 
                @sCampo5 = ?, @sCampo6 = ?, @sCampo7 = ?, @sCampo8 = ?, @sRevisado = ?, @sTrasnfe = ?, @sco_sucu_in = ?, @sco_us_in = ?, @sMaquina = ?
            """
            params = (t.tip_cli, t.des_tipo, t.co_precio, t.campo1, t.campo2, t.campo3, t.campo4, t.campo5, t.campo6, t.campo7, t.campo8, 
                t.revisado, t.trasnfe, t.co_sucu_in, 'SYNC', socket.gethostname())

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

def update_client_type (item, connect_sec):
    status = 1

    try:
        # intento de conexion a la base secundaria
        con_sec = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}}; SERVER={connect_sec["server"]}; DATABASE={connect_sec["database"]}; UID={connect_sec["username"]}; PWD={connect_sec["password"]}')
    except:
        # error al conectar a la base secundaria
        status = 0
    else:

        # se inicializa el cursor y se busca el tipo de cliente
        cursor_sec = con_sec.cursor()
        t = search_client_type(cursor_sec, item.ItemID)

        if t is None:
            # el tipo de cliente no esta en la base secundaria
            status = 2
        else:

            if item.NuevoValor is None:
                query = f"update saTipoCliente set {item.CampoModificado} = NULL, co_us_mo = 'SYNC' where tip_cli = '{item.ItemID}'"
            else:
                if item.TipoDato == 'string' or item.TipoDato == 'bool':
                    query = f"update saTipoCliente set {item.CampoModificado} = '{item.NuevoValor}', co_us_mo = 'SYNC' where tip_cli = '{item.ItemID}'"
                elif item.TipoDato == 'int' or item.TipoDato == 'decimal':
                    query = f"update saTipoCliente set {item.CampoModificado} = {item.NuevoValor}, co_us_mo = 'SYNC' where tip_cli = '{item.ItemID}'"

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

def delete_client_type (item, connect_sec):
    status = 1

    try:
        # intento de conexion a la base secundaria
        con_sec = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}}; SERVER={connect_sec["server"]}; DATABASE={connect_sec["database"]}; UID={connect_sec["username"]}; PWD={connect_sec["password"]}')
    except:
        # error al conectar a la base secundaria
        status = 0
    else:

        # se inicializa el cursor y se busca el tipo de cliente
        cursor_sec = con_sec.cursor()
        t = search_client_type(cursor_sec, item.ItemID)

        if t is None:
            # el tipo de cliente no esta en la base secundaria
            status = 2
        else:

            sp = f"exec pEliminarTipoCliente @stip_cliori = ?, @tsvalidador = ?, @smaquina = ?, @sco_us_mo = ?, @sco_sucu_mo = ?, @growguid = ?"
            params = (t.tip_cli, t.validador, socket.gethostname(), 'SYNC', t.co_sucu_mo, t.rowguid)

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

def search_client_type (cursor: Cursor, id):
    cursor.execute(f"select * from saTipoCliente where tip_cli = '{id}'")
    c = cursor.fetchone()

    return c