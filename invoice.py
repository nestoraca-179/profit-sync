import pyodbc
import socket
import reng_invoice
import sale_doc
import messages as msg
from pyodbc import Cursor

def insert_invoice (i, items, doc, connect_sec):
    status = 1

    try:
        # intento de conexion a la base secundaria
        con_sec = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}}; SERVER={connect_sec["server"]}; DATABASE={connect_sec["database"]}; UID={connect_sec["username"]}; PWD={connect_sec["password"]}')
        con_sec.autocommit = False
    except:
        # error al conectar a la base secundaria
        status = 0
    else:

        # se inicializa el cursor y se busca la factura
        cursor_sec = con_sec.cursor()
        inv = search_invoice(cursor_sec, i.doc_num)

        if inv is not None:
            # la factura ya esta en la base secundaria
            status = 2
        else:
            sp_invoice = f"""exec pInsertarFacturaVenta @sN_Control = ?, @sDoc_Num = ?, @sDescrip = ?, @sCo_Cli = ?, @sCo_Tran = ?, @sCo_Cond = ?,
                @sCo_Ven =  ?, @sCo_Cta_Ingr_Egr = ?, @sCo_Mone = ?, @bAnulado = ?,@sdFec_Emis = ?, @sdFec_Reg = ?, @sdFec_Venc = ?, @sStatus = ?, 
                @deTasa = ?, @sPorc_Desc_Glob = ?, @deMonto_Desc_Glob = ?, @sPorc_Reca = ?, @deMonto_Reca = ?, @deSaldo = ?, @deTotal_Bruto = ?, 
                @deMonto_Imp = ?, @deMonto_Imp2 = ?, @deMonto_Imp3 = ?, @deOtros1 = ?, @deOtros2 = ?, @deOtros3 = ?, @deTotal_Neto = ?, @sComentario = ?, 
                @sDir_Ent = ?, @bContrib = ?, @bImpresa = ?, @sSalestax = ?, @sImpfis = ?, @sImpfisfac = ?, @bVen_Ter = ?, @sDis_Cen = ?, @sCampo1 = ?, 
                @sCampo2 = ?, @sCampo3 = ?, @sCampo4 = ?, @sCampo5 = ?, @sCampo6 = ?, @sCampo7 = ?, @sCampo8 = ?, @sRevisado = ?, @sTrasnfe = ?,
                @sCo_Sucu_In = ?, @sCo_Us_In = ?, @sMaquina = ?
            """
            sp_invoice_params = (i.n_control, i.doc_num, i.descrip, i.co_cli, i.co_tran, i.co_cond, i.co_ven, i.co_cta_ingr_egr, i.co_mone, i.anulado, i.fec_emis, 
                i.fec_reg, i.fec_venc, i.status, i.tasa, i.porc_desc_glob, i.monto_desc_glob, i.porc_reca, i.monto_reca, i.saldo, i.total_bruto, 
                i.monto_imp, i.monto_imp2, i.monto_imp3, i.otros1, i.otros2, i.otros3, i.total_neto, i.comentario, i.dir_ent, i.contrib, i.impresa, 
                i.salestax, i.impfis, i.impfisfac, i.ven_ter, i.dis_cen, i.campo1, i.campo2, i.campo3, i.campo4, i.campo5, i.campo6, i.campo7, 
                i.campo8, i.revisado, i.trasnfe, i.co_sucu_in, 'SYNC', socket.gethostname())
            
            sp_doc = f"""exec pInsertarDocumentoVenta @sNro_Doc = ?, @sCo_Tipo_Doc = ?, @sDoc_Orig = ?, @sCo_Cli = ?, @sCo_Mone = ?, @sdFec_Reg = ?, 
                @sdFec_Emis = ?, @bAnulado = ?, @deAdicional = ?, @sMov_Ban = ?, @bAut = ?, @bContrib = ?, @sObserva = ?, @sNro_Orig = ?, @sNro_Che = ?, 
                @sCo_Ven = ?, @sCo_Cta_Ingr_Egr = ?, @deTasa = ?, @sTipo_Imp = ?, @deTotal_Bruto = ?, @deTotal_Neto = ?, @deMonto_Imp = ?, @deMonto_Imp2 = ?, 
                @deMonto_Imp3 = ?, @deSaldo = ?, @sN_Control = ?, @sNum_Comprobante = ?, @sDis_Cen = ?, @deComis1 = ?, @deComis2 = ?, @deComis3 = ?, 
                @deComis4 = ?, @deComis5 = ?, @deComis6 = ?, @deOtros1 = ?, @deOtros2 = ?, @deOtros3 = ?, @sPorc_Desc_Glob = ?, @deMonto_Desc_Glob = ?, 
                @sPorc_Reca = ?, @deMonto_Reca = ?, @dePorc_Imp = ?, @dePorc_Imp2 = ?, @dePorc_Imp3 = ?, @sSalestax = ?, @bVen_Ter = ?, @sdFec_Venc = ?, 
                @sImpFis = ?, @sImpFisFac = ?, @sImp_Nro_Z = ?, @iTipo_Origen = ?, @sCampo1 = ?, @sCampo2 = ?, @sCampo3 = ?, @sCampo4 = ?, @sCampo5 = ?, 
                @sCampo6 = ?, @sCampo7 = ?, @sCampo8 = ?, @sRevisado = ?, @sTrasnfe = ?, @sCo_Sucu_In = ?, @sCo_Us_In = ?, @sMaquina = ?
            """
            sp_doc_params = (doc.nro_doc, 'FACT', doc.doc_orig, doc.co_cli, doc.co_mone, doc.fec_reg, doc.fec_emis, doc.anulado, doc.adicional, 
                doc.mov_ban, doc.aut, doc.contrib, doc.observa, doc.nro_orig, doc.nro_che, doc.co_ven, doc.co_cta_ingr_egr, doc.tasa, doc.tipo_imp, 
                doc.total_bruto, doc.total_neto, doc.monto_imp, doc.monto_imp2, doc.monto_imp3, doc.saldo, doc.n_control, doc.num_comprobante, doc.dis_cen, 
                doc.comis1, doc.comis2, doc.comis3, doc.comis4, doc.comis5, doc.comis6, doc.otros1, doc.otros2, doc.otros3, doc.porc_desc_glob, 
                doc.monto_desc_glob, doc.porc_reca, doc.monto_reca, doc.porc_imp, doc.porc_imp2, doc.porc_imp3, doc.salestax, doc.ven_ter, 
                doc.fec_venc, doc.impfis, doc.impfisfac, doc.imp_nro_z, doc.tipo_origen, doc.campo1, doc.campo2, doc.campo3, doc.campo4, doc.campo5, 
                doc.campo6, doc.campo7, doc.campo8, doc.revisado, doc.trasnfe, doc.co_sucu_in, 'SYNC', socket.gethostname())

            try:
                # ingresando la factura
                cursor_sec.execute(sp_invoice, sp_invoice_params)
                
                # ingresando items de la factura
                for item in items:
                    sp_item = f"""exec pInsertarRenglonesFacturaVenta @sDis_Cen = ?, @sDoc_Num = ?, @sCo_Art = ?, @sDes_Art = ?, @sCo_Uni = ?, @sSco_Uni = ?, 
                        @sCo_Alma = ?, @sCo_Precio = ?, @sTipo_Imp = ?, @sTipo_Imp2 = ?, @sTipo_Imp3 = ?, @deTotal_Art = ?, @deSTotal_Art = ?, @dePrec_Vta = ?, 
                        @sPorc_Desc = ?, @deMonto_Desc = ?, @dePorc_Imp = ?, @dePorc_Imp2 = ?, @dePorc_Imp3 = ?, @deReng_Neto = ?, @dePendiente = ?, @dePendiente2 = ?, 
                        @sTipo_Doc = ?, @gRowguid_Doc = ?, @sNum_Doc = ?, @deMonto_Imp = ?, @deMonto_Imp2 = ?, @deMonto_Imp3 = ?, @deTotal_Dev = ?, @deMonto_Dev = ?, 
                        @deOtros = ?, @sComentario = ?, @deMonto_Desc_Glob = ?, @deMonto_reca_Glob = ?, @deOtros1_glob = ?, @deOtros2_glob = ?, @deOtros3_glob = ?, 
                        @deMonto_imp_afec_glob = ?, @deMonto_imp2_afec_glob = ?, @deMonto_imp3_afec_glob = ?, @iReng_Num = ?, @sREVISADO = ?, @sTRASNFE = ?, 
                        @sCo_Sucu_In = ?, @sCo_Us_In = ?, @sMaquina = ?
                    """
                    sp_item_params = (item.dis_cen, item.doc_num, item.co_art, item.des_art, item.co_uni, item.sco_uni, item.co_alma, item.co_precio, item.tipo_imp,
                        item.tipo_imp2, item.tipo_imp3, item.total_art, item.stotal_art, item.prec_vta, item.porc_desc, item.monto_desc, item.porc_imp, 
                        item.porc_imp2, item.porc_imp3, item.reng_neto, item.pendiente, item.pendiente2, item.tipo_doc, item.rowguid_doc, item.num_doc, 
                        item.monto_imp, item.monto_imp2, item.monto_imp3, item.total_dev, item.monto_dev, item.otros, item.comentario, item.monto_desc_glob, 
                        item.monto_reca_glob, item.otros1_glob, item.otros2_glob, item.otros3_glob, item.monto_imp_afec_glob, item.monto_imp2_afec_glob, 
                        item.monto_imp3_afec_glob, item.reng_num, item.revisado, item.trasnfe, item.co_sucu_in, 'SYNC', socket.gethostname())
                    
                    cursor_sec.execute(sp_item, sp_item_params)

                # ingresando el documento de venta
                cursor_sec.execute(sp_doc, sp_doc_params)

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

def update_invoice (item, connect_sec):
    status = 1

    try:
        # intento de conexion a la base secundaria
        con_sec = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}}; SERVER={connect_sec["server"]}; DATABASE={connect_sec["database"]}; UID={connect_sec["username"]}; PWD={connect_sec["password"]}')
    except:
        # error al conectar a la base secundaria
        status = 0
    else:

        # se inicializa el cursor y se busca la factura
        cursor_sec = con_sec.cursor()
        inv = search_invoice(cursor_sec, item.ItemID)

        if inv is None:
            # la factura no esta en la base secundaria
            status = 2
        else:

            if item.NuevoValor is None:
                query = f"""update saFacturaVenta set {item.CampoModificado} = NULL, co_us_mo = 'SYNC' where doc_num = '{item.ItemID}'"""
            else:
                if item.TipoDato == 'string' or item.TipoDato == 'bool':
                    query = f"""update saFacturaVenta set {item.CampoModificado} = '{item.NuevoValor}', co_us_mo = 'SYNC' 
                                where doc_num = '{item.ItemID}'"""
                elif item.TipoDato == 'int' or item.TipoDato == 'decimal':
                    query = f"""update saFacturaVenta set {item.CampoModificado} = {item.NuevoValor}, co_us_mo = 'SYNC' 
                                where doc_num = '{item.ItemID}'"""

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

def delete_invoice (item, connect_sec):
    status = 1

    try:
        # intento de conexion a la base secundaria
        con_sec = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}}; SERVER={connect_sec["server"]}; DATABASE={connect_sec["database"]}; UID={connect_sec["username"]}; PWD={connect_sec["password"]}')
        con_sec.autocommit = False
    except:
        # error al conectar a la base secundaria
        status = 0
    else:

        # se inicializa el cursor y se busca la factura y sus elementos
        cursor_sec = con_sec.cursor()
        inv = search_invoice(cursor_sec, item.ItemID)
        inv_items = reng_invoice.search_all_invoice_items(cursor_sec, item.ItemID)
        inv_doc = sale_doc.search_sale_doc(cursor_sec, 'FACT', item.ItemID)

        if inv is None:
            # la factura no esta en la base secundaria
            status = 2
        else:
            sp_i = f"exec pEliminarFacturaVenta @sdoc_numori = ?, @tsvalidador = ?, @smaquina = ?, @sco_us_mo = ?, @sco_sucu_mo = ?, @growguid = ?"
            sp_i_params = (inv.doc_num, inv.validador, socket.gethostname(), 'SYNC', None, inv.rowguid)

            sp_i_doc = f"""exec pEliminarDocumentoVenta @sco_tipo_docori = ?, @snro_docori = ?,
                @tsvalidador = ?, @smaquina = ?, @sco_us_mo = ?, @sco_sucu_mo = ?, @growguid = ?
            """
            sp_i_doc_params = ('FACT', inv_doc.nro_doc, inv_doc.validador, socket.gethostname(), 'SYNC', None, inv_doc.rowguid)

            try:
                # ejecucion de script
                cursor_sec.execute(sp_i, sp_i_params)

                for item in inv_items:
                    sp_inv_item = f"""exec pEliminarRenglonesFacturaVenta @sdoc_numori = ?, @ireng_numori = ?, @sco_us_mo = ?,
                        @smaquina = ?, @sco_sucu_mo = ?, @growguid = ?
                    """
                    sp_inv_item_params = (item.doc_num, item.reng_num, 'SYNC', socket.gethostname(), None, item.rowguid)

                    cursor_sec.execute(sp_inv_item, sp_inv_item_params)

                cursor_sec.execute(sp_i_doc, sp_i_doc_params)
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

def search_invoice (cursor: Cursor, id):
    cursor.execute(f"select * from saFacturaVenta where doc_num = '{id}'")
    i = cursor.fetchone()

    return i