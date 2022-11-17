import pyodbc
import socket
import messages as msg
from pyodbc import Cursor

def insert_reng_invoice (item, connect_sec):
    status = 1

    try:
        # intento de conexion a la base secundaria
        con_sec = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}}; SERVER={connect_sec["server"]}; DATABASE={connect_sec["database"]}; UID={connect_sec["username"]}; PWD={connect_sec["password"]}')
    except:
        # error al conectar a la base secundaria
        status = 0
    else:

        # se inicializa el cursor y se busca el renglon
        cursor_sec = con_sec.cursor()
        r = search_reng_invoice(cursor_sec, item.doc_num, item.reng_num)

        if r is not None:
            # el renglon ya esta en la base secundaria
            status = 2
        else:
            sp_item = f"""exec pInsertarRenglonesFacturaVenta @sdis_cen = ?, @sDoc_Num = ?, @sCo_Art = ?, @sDes_Art = ?, @sCo_Uni = ?, @sSco_Uni = ?, 
                @sCo_Alma = ?, @sCo_Precio = ?, @sTipo_Imp = ?, @sTipo_Imp2 = ?, @sTipo_Imp3 = ?, @deTotal_Art = ?, @deStotal_Art = ?, @dePrec_Vta = ?, 
                @sPorc_Desc = ?, @deMonto_Desc = ?, @dePorc_Imp = ?, @dePorc_Imp2 = ?, @dePorc_Imp3 = ?, @deReng_Neto = ?, @dePendiente = ?, @dePendiente2 = ?, 
                @sTipo_Doc = ?, @gRowguid_Doc = ?, @sNum_Doc = ?, @deMonto_Imp = ?, @deMonto_Imp2 = ?, @deMonto_Imp3 = ?, @deTotal_Dev = ?, @deMonto_Dev = ?, 
                @deOtros = ?, @sComentario = ?, @deMonto_Desc_Glob = ?, @deMonto_Reca_Glob = ?, @deOtros1_Glob = ?, @deOtros2_glob = ?, @deOtros3_glob = ?, 
                @deMonto_imp_afec_glob = ?, @deMonto_imp2_afec_glob = ?, @deMonto_imp3_afec_glob = ?, @iRENG_NUM = ?, @sREVISADO = ?, @sTRASNFE = ?, 
                @sco_sucu_in = ?, @sco_us_in = ?, @sMaquina = ?
            """
            sp_item_params = (None, item.doc_num, item.co_art, item.des_art, item.co_uni, item.sco_uni, item.co_alma, item.co_precio, item.tipo_imp,
                item.tipo_imp2, item.tipo_imp3, item.total_art, item.stotal_art, item.prec_vta, item.porc_desc, item.monto_desc, item.porc_imp, 
                item.porc_imp2, item.porc_imp3, item.reng_neto, item.pendiente, item.pendiente2, item.tipo_doc, item.rowguid_doc, item.num_doc, 
                item.monto_imp, item.monto_imp2, item.monto_imp3, item.total_dev, item.monto_dev, item.otros, item.comentario, item.monto_desc_glob, 
                item.monto_reca_glob, item.otros1_glob, item.otros2_glob, item.otros3_glob, item.monto_imp_afec_glob, item.monto_imp2_afec_glob, 
                item.monto_imp3_afec_glob, item.reng_num, item.revisado, item.trasnfe, item.co_sucu_in, 'SYNC', socket.gethostname())

            try:
                # ejecucion de script
                cursor_sec.execute(sp_item, sp_item_params)
                cursor_sec.commit()
            except pyodbc.Error as error:
                # error en la ejecucion
                msg.print_error_msg(error)
                status = 3
                pass

        cursor_sec.close()
        con_sec.close()

    return status

def update_reng_invoice (item, fact, reng, connect_sec):
    status = 1

    try:
        # intento de conexion a la base secundaria
        con_sec = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}}; SERVER={connect_sec["server"]}; DATABASE={connect_sec["database"]}; UID={connect_sec["username"]}; PWD={connect_sec["password"]}')
    except:
        # error al conectar a la base secundaria
        status = 0
    else:

        # se inicializa el cursor y se busca el renglon
        cursor_sec = con_sec.cursor()
        r = search_reng_invoice(cursor_sec, fact, reng)

        if r is None:
            # el renglon no esta en la base secundaria
            status = 2
        else:

            if item.NuevoValor is None:
                query = f"""update saFacturaVentaReng set {item.CampoModificado} = NULL, co_us_mo = 'SYNC'
                            where doc_num = '{fact}' and reng_num = '{reng}'"""
            else:
                if item.TipoDato == 'string' or item.TipoDato == 'bool':
                    query = f"""update saFacturaVentaReng set {item.CampoModificado} = '{item.NuevoValor}', co_us_mo = 'SYNC'
                                where doc_num = '{fact}' and reng_num = '{reng}'"""
                elif item.TipoDato == 'int' or item.TipoDato == 'decimal':
                    query = f"""update saFacturaVentaReng set {item.CampoModificado} = {item.NuevoValor}, co_us_mo = 'SYNC'
                                where doc_num = '{fact}' and reng_num = '{reng}'"""

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

def delete_reng_invoice (fact, reng, connect_sec):
    status = 1

    try:
        # intento de conexion a la base secundaria
        con_sec = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}}; SERVER={connect_sec["server"]}; DATABASE={connect_sec["database"]}; UID={connect_sec["username"]}; PWD={connect_sec["password"]}')
    except:
        # error al conectar a la base secundaria
        status = 0
    else:

        # se inicializa el cursor y se busca el renglon
        cursor_sec = con_sec.cursor()
        r = search_reng_invoice(cursor_sec, fact, reng)

        if r is None:
            # el renglon no esta en la base secundaria
            status = 2
        else:
            sp_r = f"""exec pEliminarRenglonesFacturaVenta @sdoc_numori = ?, @ireng_numori = ?, @sco_us_mo = ?, @smaquina = ?, 
                @sco_sucu_mo = ?, @growguid = ?
            """
            sp_r_params = (fact, reng, 'SYNC', socket.gethostname(), None, r.rowguid)

            try:
                # ejecucion de script
                cursor_sec.execute(sp_r, sp_r_params)
                cursor_sec.commit()
            except pyodbc.Error as error:
                # error en la ejecucion
                msg.print_error_msg(error)
                status = 3
                pass

        cursor_sec.close()
        con_sec.close()

    return status

def search_reng_invoice (cursor: Cursor, id, reng):
    cursor.execute(f"select * from saFacturaVentaReng where doc_num = '{id}' and reng_num = {reng}")
    new_reng = cursor.fetchone()

    return new_reng

def search_all_invoice_items (cursor: Cursor, id):
    cursor.execute(f"select * from saFacturaVentaReng where doc_num = '{id}'")
    items = cursor.fetchall()

    return items