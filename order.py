import pyodbc
import socket
import messages as msg
from pyodbc import Cursor

def insert_order (p, items, connect_sec):
    status = 1

    try:
        # intento de conexion a la base secundaria
        con_sec = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}}; SERVER={connect_sec["server"]}; DATABASE={connect_sec["database"]}; UID={connect_sec["username"]}; PWD={connect_sec["password"]}')
        con_sec.autocommit = False
    except:
        # error al conectar a la base secundaria
        status = 0
    else:

        # se inicializa el cursor y se busca el pedido
        cursor_sec = con_sec.cursor()
        ord = search_order(cursor_sec, p.doc_num)

        if ord is not None:
            # el pedido ya esta en la base secundaria
            status = 2
        else:
            sp_order = f"""exec pInsertarPedidoVenta @sdFec_Emis = ?, @sDoc_Num = ?, @sDescrip = ?, @sCo_Cli = ?, @sCo_Tran = ?, @sCo_Cond = ?, 
                @sCo_Ven = ?, @sCo_Cta_Ingr_Egr = ?, @sCo_Mone = ?, @bAnulado = ?, @sdFec_Reg = ?, @sdFec_Venc = ?, @sStatus = ?, @deTasa = ?, 
                @sN_Control = ?, @sPorc_Desc_Glob = ?, @deMonto_Desc_Glob = ?, @sPorc_Reca = ?, @deMonto_Reca = ?, @deSaldo = ?, @deTotal_Bruto = ?, 
                @deMonto_Imp = ?, @deMonto_Imp3 = ?, @deOtros1 = ?, @deOtros2 = ?, @deOtros3 = ?, @deMonto_Imp2 = ?, @deTotal_Neto = ?, @sComentario = ?, 
                @sDir_Ent = ?, @bContrib = ?, @bImpresa = ?, @sSalestax = ?, @sImpfis = ?, @sImpfisfac = ?, @bVen_Ter = ?, @sDis_Cen = ?, @sCampo1 = ?, 
                @sCampo2 = ?, @sCampo3 = ?, @sCampo4 = ?, @sCampo5 = ?, @sCampo6 = ?, @sCampo7 = ?, @sCampo8 = ?, @sRevisado = ?, @sTrasnfe = ?, 
                @sCo_Sucu_In = ?, @sCo_Us_In = ?, @sMaquina = ?
            """
            sp_order_params = (p.fec_emis, p.doc_num, p.descrip, p.co_cli, p.co_tran, p.co_cond, p.co_ven, p.co_cta_ingr_egr, p.co_mone, p.anulado, 
                p.fec_reg, p.fec_venc, p.status, p.tasa, p.n_control, p.porc_desc_glob, p.monto_desc_glob, p.porc_reca, p.monto_reca, p.saldo, 
                p.total_bruto, p.monto_imp, p.monto_imp3, p.otros1, p.otros2, p.otros3, p.monto_imp2, p.total_neto, p.comentario, p.dir_ent, 
                p.contrib, p.impresa, p.salestax, p.impfis, p.impfisfac, p.ven_ter, p.dis_cen, p.campo1, p.campo2, p.campo3, p.campo4, p.campo5, 
                p.campo6, p.campo7, p.campo8, p.revisado, p.trasnfe, p.co_sucu_in, 'SYNC', socket.gethostname())

            try:
                # ingresando el pedido
                cursor_sec.execute(sp_order, sp_order_params)
                
                # ingresando items del pedido
                for item in items:
                    sp_item = f"""exec pInsertarRenglonesPedidoVenta @sDoc_Num = ?, @sCo_Art = ?, @sDes_Art = ?, @sCo_Uni = ?, @sSco_Uni = ?, 
                        @sCo_Alma = ?, @sCo_Precio = ?, @sTipo_Imp = ?, @sTipo_Imp2 = ?, @sTipo_Imp3 = ?, @deTotal_Art = ?, @deStotal_Art = ?, 
                        @dePrec_Vta = ?, @sPorc_Desc = ?, @deMonto_Desc = ?, @dePorc_Imp = ?, @dePorc_Imp2 = ?, @dePorc_Imp3 = ?, @deReng_Neto = ?, 
                        @dePendiente = ?, @dePendiente2 = ?, @sTipo_Doc = ?, @gRowguid_Doc = ?, @sNum_Doc = ?, @deMonto_Imp = ?, @deTotal_Dev = ?, 
                        @deMonto_Dev = ?, @deOtros = ?, @deMonto_Imp2 = ?, @deMonto_Imp3 = ?, @sComentario = ?, @sDis_Cen = ?, @deMonto_Desc_Glob = ?, 
                        @deMonto_Reca_Glob = ?, @deOtros1_Glob = ?, @deOtros2_glob = ?, @deOtros3_glob = ?, @deMonto_imp_afec_glob = ?, 
                        @deMonto_imp2_afec_glob = ?, @deMonto_imp3_afec_glob = ?, @iRENG_NUM = ?, @sREVISADO = ?, @sTRASNFE = ?, @sCo_Sucu_In = ?, 
                        @sCo_Us_In = ?, @sMaquina = ?
                    """
                    sp_item_params = (item.doc_num, item.co_art, item.des_art, item.co_uni, item.sco_uni, item.co_alma, item.co_precio, item.tipo_imp, 
                        item.tipo_imp2, item.tipo_imp3, item.total_art, item.stotal_art, item.prec_vta, item.porc_desc, item.monto_desc, item.porc_imp, 
                        item.porc_imp2, item.porc_imp3, item.reng_neto, item.pendiente, item.pendiente2, item.tipo_doc, item.rowguid_doc, item.num_doc, 
                        item.monto_imp, item.total_dev, item.monto_dev, item.otros, item.monto_imp2, item.monto_imp3, item.comentario, item.dis_cen, 
                        item.monto_desc_glob, item.monto_reca_glob, item.otros1_glob, item.otros2_glob, item.otros3_glob, item.monto_imp_afec_glob, 
                        item.monto_imp2_afec_glob, item.monto_imp3_afec_glob, item.reng_num, item.revisado, item.trasnfe, item.co_sucu_in, 'SYNC', 
                        socket.gethostname())
                    
                    cursor_sec.execute(sp_item, sp_item_params)

                # commit de script
                con_sec.commit()

            except pyodbc.Error as error:
                # error en la ejecucion
                msg.print_error_msg(error)
                con_sec.rollback()
                status = 3
                pass

        cursor_sec.close()
        con_sec.close()

    return status

def update_order (item, connect_sec):
    status = 1

    try:
        # intento de conexion a la base secundaria
        con_sec = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}}; SERVER={connect_sec["server"]}; DATABASE={connect_sec["database"]}; UID={connect_sec["username"]}; PWD={connect_sec["password"]}')
    except:
        # error al conectar a la base secundaria
        status = 0
    else:

        # se inicializa el cursor y se busca el pedido
        cursor_sec = con_sec.cursor()
        ord = search_order(cursor_sec, item.ItemID)

        if ord is None:
            # el pedido no esta en la base secundaria
            status = 2
        else:

            if item.NuevoValor is None:
                query = f"update saPedidoVenta set {item.CampoModificado} = NULL, co_us_mo = 'SYNC' where doc_num = '{item.ItemID}'"
            else:
                if item.TipoDato == 'string' or item.TipoDato == 'bool':
                    query = f"update saPedidoVenta set {item.CampoModificado} = '{item.NuevoValor}', co_us_mo = 'SYNC' where doc_num = '{item.ItemID}'"
                elif item.TipoDato == 'int' or item.TipoDato == 'decimal':
                    query = f"update saPedidoVenta set {item.CampoModificado} = {item.NuevoValor}, co_us_mo = 'SYNC' where doc_num = '{item.ItemID}'"

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

def update_reng_order (item, ord, reng, connect_sec):
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
        r = search_reng_order(cursor_sec, ord, reng)

        if r is None:
            # el renglon no esta en la base secundaria
            status = 2
        else:

            if item.NuevoValor is None:
                query = f"""update saPedidoVentaReng set {item.CampoModificado} = NULL, co_us_mo = 'SYNC'
                            where doc_num = '{ord}' and reng_num = '{reng}'"""
            else:
                if item.TipoDato == 'string' or item.TipoDato == 'bool':
                    query = f"""update saPedidoVentaReng set {item.CampoModificado} = '{item.NuevoValor}', co_us_mo = 'SYNC'
                                where doc_num = '{ord}' and reng_num = '{reng}'"""
                elif item.TipoDato == 'int' or item.TipoDato == 'decimal':
                    query = f"""update saPedidoVentaReng set {item.CampoModificado} = {item.NuevoValor}, co_us_mo = 'SYNC'
                                where doc_num = '{ord}' and reng_num = '{reng}'"""

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

def delete_order (item, connect_sec):
    status = 1

    try:
        # intento de conexion a la base secundaria
        con_sec = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}}; SERVER={connect_sec["server"]}; DATABASE={connect_sec["database"]}; UID={connect_sec["username"]}; PWD={connect_sec["password"]}')
        con_sec.autocommit = False
    except:
        # error al conectar a la base secundaria
        status = 0
    else:

        # se inicializa el cursor y se busca el pedido y sus elementos
        cursor_sec = con_sec.cursor()
        ord = search_order(cursor_sec, item.ItemID)
        ord_items = search_all_order_items(cursor_sec, item.ItemID)

        if ord is None:
            # el pedido no esta en la base secundaria
            status = 2
        else:
            sp_o = f"exec pEliminarPedidoVenta @sdoc_numori = ?, @tsvalidador = ?, @smaquina = ?, @sco_us_mo = ?, @sco_sucu_mo = ?, @growguid = ?"
            sp_o_params = (ord.doc_num, ord.validador, socket.gethostname(), 'SYNC', ord.co_sucu_mo, ord.rowguid)

            try:
                # ejecucion de script
                cursor_sec.execute(sp_o, sp_o_params)

                for item in ord_items:
                    sp_o_item = f"""exec pEliminarRenglonesPedidoVenta @sdoc_numori = ?, @ireng_numori = ?, @sco_us_mo = ?,
                        @smaquina = ?, @sco_sucu_mo = ?, @growguid = ?
                    """
                    sp_o_item_params = (item.doc_num, item.reng_num, 'SYNC', socket.gethostname(), None, item.rowguid)

                    cursor_sec.execute(sp_o_item, sp_o_item_params)

                cursor_sec.commit()

            except pyodbc.Error as error:
                # error en la ejecucion
                msg.print_error_msg(error)
                con_sec.rollback()
                status = 3
                pass

        cursor_sec.close()
        con_sec.close()

    return status

def search_order (cursor: Cursor, id):
    cursor.execute(f"select * from saPedidoVenta where doc_num = '{id}'")
    p = cursor.fetchone()

    return p

def search_reng_order (cursor: Cursor, id, reng):
    cursor.execute(f"select * from saPedidoVentaReng where doc_num = '{id}' and reng_num = {reng}")
    new_reng = cursor.fetchone()

    return new_reng

def search_all_order_items (cursor: Cursor, id):
    cursor.execute(f"select * from saPedidoVentaReng where doc_num = '{id}'")
    items = cursor.fetchall()

    return items