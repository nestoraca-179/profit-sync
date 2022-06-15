import pyodbc
import socket
import messages as msg
from pyodbc import Cursor

def insert_collect(c, rengs_doc, rengs_tp, cursor_main: Cursor, connect_sec):
    status = 1

    try:
        # intento de conexion a la base secundaria
        con_sec = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}}; SERVER={connect_sec["server"]}; DATABASE={connect_sec["database"]}; UID={connect_sec["username"]}; PWD={connect_sec["password"]}')
        con_sec.autocommit = False
    except:
        # error al conectar a la base secundaria
        status = 0
    else:

        # se inicializa el cursor y se busca el cobro y el cliente
        cursor_sec = con_sec.cursor()
        cob = search_collect(cursor_sec, c.cob_num)

        if cob is not None:
            # el cobro ya esta en la base secundaria
            status = 2
        else:
            sp_collect = f"""exec pInsertarCobro @sCob_Num = ?, @sRecibo = ?, @sCo_cli = ?, @sCo_Ven = ?, @sCo_Mone = ?, @deTasa = ?,
                @sdfecha = ?, @bAnulado = ?, @deMonto = ?, @sDis_Cen = ?, @sDescrip = ?, @sCampo1 = ?, @sCampo2 = ?, @sCampo3 = ?, 
                @sCampo4 = ?, @sCampo5 = ?, @sCampo6 = ?, @sCampo7 = ?, @sCampo8 = ?, @sRevisado = ?, @sTrasnfe = ?, @sco_sucu_in = ?,
                @sco_us_in = ?, @sMaquina = ?
            """
            sp_collect_params = (c.cob_num, c.recibo, c.co_cli, c.co_ven, c.co_mone, c.tasa, c.fecha, c.anulado, c.monto, c.dis_cen, 
                c.descrip, c.campo1, c.campo2, c.campo3, c.campo4, c.campo5, c.campo6, c.campo7, c.campo8, c.revisado, c.trasnfe,
                c.co_sucu_in, c.co_us_in, socket.gethostname())

            try:
                rengs_iva = []
                rengs_islr = []

                # actualizando saldos de caja
                for tp in rengs_tp:

                    code = tp.cod_cta if tp.cod_cta is not None else tp.cod_caja
                    tipo_saldo = "EF" if tp.forma_pag == "EF" else "TF"

                    sp_tp = f"""exec pSaldoActualizar @sCodigo = ?,@sForma_Pag = ?, @sTipoSaldo = ?, @deMonto = ?, @bSumarSaldo = 1,
                        @sModulo = N'COBRO', @bPermiteSaldoNegativo = 0
                    """
                    sp_tp_params = (code, tp.forma_pag, tipo_saldo, tp.mont_doc)

                    cursor_sec.execute(sp_tp, sp_tp_params)

                movs_c = search_movs_c(cursor_main, c.cob_num) # movimientos de caja
                movs_b = search_movs_b(cursor_main, c.cob_num) # movimientos de banco

                # ingresando movimientos de caja
                for m in movs_c:
                    tipo_mov = 'MOVC_NUM'

                    # cursor_sec.execute(f"""
                    #    set nocount on
                    #    exec pConsecutivoProximo @sCo_Consecutivo=N'{tipo_mov}', @sCo_Sucur=N'{c.co_sucu_in}'
                    # """)
                    # consec = cursor_sec.fetchone().ProximoConsecutivo
                    # print(consec)

                    sp_mov = f"""exec pInsertarMovimientoCaja @smov_num = ?, @sdFecha = ?, @sdescrip = ?, @scod_caja = ?, @detasa = ?, 
                        @stipo_mov = ?, @sforma_pag = ?, @snum_pago = ?, @sco_ban = ?, @sco_tar = ?, @sco_vale = ?, 
                        @sco_cta_ingr_egr = ?, @demonto = ?, @bsaldo_ini = ?, @sorigen = ?, @sdoc_num = ?, @sDep_Num = ?, @banulado = ?, 
                        @bDepositado = ?, @bConciliado = ?, @btransferido = ?, @smov_nro = ?, @sdfecha_che = ?, @sdis_cen = ?, @sCampo1 = ?, 
                        @sCampo2 = ?, @sCampo3 = ?, @sCampo4 = ?, @sCampo5 = ?, @sCampo6 = ?, @sCampo7 = ?, @sCampo8 = ?, @sRevisado = ?, 
                        @sTrasnfe = ?, @sco_sucu_in = ?, @sco_us_in = ?, @sMaquina = ?
                    """
                    sp_mov_params = (m.mov_num, m.fecha, 'Cobro ' + c.cob_num, m.cod_caja, m.tasa, m.tipo_mov, m.forma_pag, m.num_pago, 
                        m.co_ban, m.co_tar, m.co_vale, m.co_cta_ingr_egr, m.monto_h, m.saldo_ini, 'COBRO', m.doc_num, m.dep_num, 
                        m.anulado, m.depositado, 0, m.transferido, m.mov_nro, m.fecha_che, m.dis_cen, m.campo1, m.campo2, m.campo3, 
                        m.campo4, m.campo5, m.campo6, m.campo7, m.campo8, m.revisado, m.trasnfe, m.co_sucu_in, m.co_us_in, socket.gethostname())

                    cursor_sec.execute(sp_mov, sp_mov_params)

                # ingresando movimientos de banco
                for m in movs_b:
                    tipo_mov = 'MOVB_NUM'

                    # cursor_sec.execute(f"""
                    #    set nocount on
                    #    exec pConsecutivoProximo @sCo_Consecutivo=N'{tipo_mov}', @sCo_Sucur=N'{c.co_sucu_in}'
                    # """)
                    # consec = cursor_sec.fetchone().ProximoConsecutivo
                    # print(consec)

                    sp_mov = f"""exec pInsertarMovimientoBanco @sMov_Num = ?, @sDescrip = ?, @sCod_Cta = ?, @sdFecha = ?, 
                        @deTasa = ?, @sTipo_Op = ?, @sDoc_Num = ?, @deMonto = ?, @sco_cta_ingr_egr = ?, @sOrigen = ?, @sCob_Pag = ?, @deIDB = ?, 
                        @sDep_Num = ?, @bAnulado = ?, @bConciliado = ?, @bSaldo_Ini = ?, @bOri_Dep = ?, @iDep_Con = ?, @sdFec_Con = ?, 
                        @sCod_IngBen = ?, @sdFecha_Che = ?, @sDis_Cen = ?, @iNro_Transf_Nomi = ?, @sCampo1 = ?, @sCampo2 = ?, @sCampo3 = ?, 
                        @sCampo4 = ?, @sCampo5 = ?, @sCampo6 = ?, @sCampo7 = ?, @sCampo8 = ?, @sRevisado = ?, @sTrasnfe = ?, @sco_sucu_in = ?, 
                        @sco_us_in = ?, @sMaquina = ?
                    """
                    sp_mov_params = (m.mov_num, 'Cobro ' + c.cob_num, m.cod_cta, m.fecha, m.tasa, m.tipo_op, m.doc_num, m.monto_h, 
                        m.co_cta_ingr_egr, 'COBRO', m.cob_pag, m.idb, m.dep_num, m.anulado, 0, m.saldo_ini, m.ori_dep, m.dep_con, 
                        m.fec_con, m.cod_ingben, m.fecha_che, m.dis_cen, m.nro_transf_nomi, m.campo1, m.campo2, m.campo3, m.campo4, 
                        m.campo5, m.campo6, m.campo7, m.campo8, m.revisado, m.trasnfe, m.co_sucu_in, m.co_us_in, socket.gethostname())

                    cursor_sec.execute(sp_mov, sp_mov_params)
                
                # ingresando el cobro
                cursor_sec.execute(sp_collect, sp_collect_params)

                # ingresando renglones de documentos
                for doc in rengs_doc:

                    sp_doc = """exec pInsertarRenglonesDocCobro @sCob_Num = ?, @sCo_Tipo_Doc = ?, @sNro_Doc = ?, @deMont_Cob = ?, 
                        @iTipo_Origen = ?, @deDpCobro_Porc_Desc = ?, @deDpCobro_Monto = ?, @demonto_retencion_iva = ?, 
                        @demonto_retencion = ?, @sTipo_Doc = ?, @sNum_Doc = ?, @growguid_reng_ori = ?, @growguid = ?, @iRENG_NUM = ?, 
                        @sREVISADO = ?, @sTRASNFE = ?, @sco_sucu_in = ?, @sco_us_in = ?, @sMaquina = ?
                    """
                    sp_doc_params = (doc.cob_num, doc.co_tipo_doc, doc.nro_doc, doc.mont_cob, doc.tipo_origen, doc.dpcobro_porc_desc,
                        doc.dpcobro_monto, doc.monto_retencion_iva, doc.monto_retencion, doc.tipo_doc, doc.num_doc, doc.rowguid_reng_ori, 
                        doc.rowguid, doc.reng_num, doc.revisado, doc.trasnfe, doc.co_sucu_in, doc.co_us_in, socket.gethostname())

                    if doc.co_tipo_doc.rstrip() == 'IVAN': # retencion de iva
                        doc_iva = search_doc_iva(cursor_main, doc.nro_doc)
                        sp_doc_iva = """exec pInsertarDocumentoVenta @sNro_Doc = ?, @sCo_Tipo_Doc = 'IVAN', @sDoc_Orig = 'COBRO', @sCo_Cli = ?, 
                            @sCo_Mone = ?, @sdFec_Reg = ?, @sdFec_Emis = ?, @bAnulado = ?, @deAdicional = ?, @sMov_Ban = ?, @bAut = ?, @bContrib = ?, 
                            @sObserva = ?, @sNro_Orig = ?, @sNro_Che = ?, @sCo_Ven = ?, @sCo_Cta_Ingr_Egr = ?, @deTasa = ?, @sTipo_Imp = ?, 
                            @deTotal_Bruto = ?, @deTotal_Neto = ?, @deMonto_Reca = ?, @deMonto_Imp = ?, @deMonto_Imp2 = ?, @deMonto_Imp3 = ?, 
                            @deSaldo = ?, @sN_Control = ?, @sNum_Comprobante = ?, @sDis_Cen = ?, @deComis1 = ?, @deComis2 = ?, @deComis3 = ?, 
                            @deComis4 = ?, @deOtros1 = ?, @deOtros2 = ?, @deOtros3 = ?, @sPorc_Desc_Glob = ?, @deMonto_Desc_Glob = ?, @sPorc_Reca = ?, 
                            @dePorc_Imp = ?, @dePorc_Imp2 = ?, @dePorc_Imp3 = ?, @sSalestax = ?, @bVen_Ter = ?, @sdFec_Venc = ?, @deComis5 = ?, 
                            @deComis6 = ?, @sImpFis = ?, @sImpFisFac = ?, @sImp_Nro_Z = ?, @iTipo_Origen = ?, @sCampo1 = ?, @sCampo2 = ?, @sCampo3 = ?,
                            @sCampo4 = ?, @sCampo5 = ?, @sCampo6 = ?, @sCampo7 = ?, @sCampo8 = ?, @sRevisado = ?, @sTrasnfe = ?, @sco_sucu_in = ?, 
                            @sco_us_in = ?, @sMaquina = ?
                        """
                        sp_doc_iva_params = (doc_iva.nro_doc, doc_iva.co_cli, doc_iva.co_mone, doc_iva.fec_reg, doc_iva.fec_emis, doc_iva.anulado, 
                            doc_iva.adicional, doc_iva.mov_ban, doc_iva.aut, doc_iva.contrib, doc_iva.observa, doc_iva.nro_orig, doc_iva.nro_che,
                            doc_iva.co_ven, doc_iva.co_cta_ingr_egr, doc_iva.tasa, doc_iva.tipo_imp, doc_iva.total_bruto, doc_iva.total_neto, 
                            doc_iva.monto_reca, doc_iva.monto_imp, doc_iva.monto_imp2, doc_iva.monto_imp3, doc_iva.saldo, doc_iva.n_control, 
                            doc_iva.num_comprobante, doc_iva.dis_cen, doc_iva.comis1, doc_iva.comis2, doc_iva.comis3, doc_iva.comis4, doc_iva.otros1, 
                            doc_iva.otros2, doc_iva.otros3, doc_iva.porc_desc_glob, doc_iva.monto_desc_glob, doc_iva.porc_reca, doc_iva.porc_imp, 
                            doc_iva.porc_imp2, doc_iva.porc_imp3, doc_iva.salestax, doc_iva.ven_ter, doc_iva.fec_venc, doc_iva.comis5, doc_iva.comis6, 
                            doc_iva.impfis, doc_iva.impfisfac, doc_iva.imp_nro_z, doc_iva.tipo_origen, doc_iva.campo1, doc_iva.campo2, doc_iva.campo3, 
                            doc_iva.campo4, doc_iva.campo5, doc_iva.campo6, doc_iva.campo7, doc_iva.campo8, doc_iva.revisado, doc_iva.trasnfe, 
                            doc_iva.co_sucu_in, doc_iva.co_us_in, socket.gethostname())

                        rengs_iva.append(doc.rowguid)
                        cursor_sec.execute(sp_doc_iva, sp_doc_iva_params)
                    
                    elif doc.co_tipo_doc.rstrip() == 'ISLR': # retencion de islr
                        doc_islr = search_doc_islr(cursor_main, doc.nro_doc)
                        sp_doc_islr = """exec pInsertarDocumentoVenta @sNro_Doc = ?, @sCo_Tipo_Doc = 'ISLR', @sDoc_Orig = 'COBRO', @sCo_Cli = ?, 
                            @sCo_Mone = ?, @sdFec_Reg = ?, @sdFec_Emis = ?, @bAnulado = ?, @deAdicional = ?, @sMov_Ban = ?, @bAut = ?, @bContrib = ?, 
                            @sObserva = ?, @sNro_Orig = ?, @sNro_Che = ?, @sCo_Ven = ?, @sCo_Cta_Ingr_Egr = ?, @deTasa = ?, @sTipo_Imp = ?, 
                            @deTotal_Bruto = ?, @deTotal_Neto = ?, @deMonto_Reca = ?, @deMonto_Imp = ?, @deMonto_Imp2 = ?, @deMonto_Imp3 = ?, 
                            @deSaldo = ?, @sN_Control = ?, @sNum_Comprobante = ?, @sDis_Cen = ?, @deComis1 = ?, @deComis2 = ?, @deComis3 = ?, 
                            @deComis4 = ?, @deOtros1 = ?, @deOtros2 = ?, @deOtros3 = ?, @sPorc_Desc_Glob = ?, @deMonto_Desc_Glob = ?, @sPorc_Reca = ?, 
                            @dePorc_Imp = ?, @dePorc_Imp2 = ?, @dePorc_Imp3 = ?, @sSalestax = ?, @bVen_Ter = ?, @sdFec_Venc = ?, @deComis5 = ?, 
                            @deComis6 = ?, @sImpFis = ?, @sImpFisFac = ?, @sImp_Nro_Z = ?, @iTipo_Origen = ?, @sCampo1 = ?, @sCampo2 = ?, @sCampo3 = ?,
                            @sCampo4 = ?, @sCampo5 = ?, @sCampo6 = ?, @sCampo7 = ?, @sCampo8 = ?, @sRevisado = ?, @sTrasnfe = ?, @sco_sucu_in = ?, 
                            @sco_us_in = ?, @sMaquina = ?
                        """
                        sp_doc_islr_params = (doc_islr.nro_doc, doc_islr.co_cli, doc_islr.co_mone, doc_islr.fec_reg, doc_islr.fec_emis, doc_islr.anulado, 
                            doc_islr.adicional, doc_islr.mov_ban, doc_islr.aut, doc_islr.contrib, doc_islr.observa, doc_islr.nro_orig, doc_islr.nro_che,
                            doc_islr.co_ven, doc_islr.co_cta_ingr_egr, doc_islr.tasa, doc_islr.tipo_imp, doc_islr.total_bruto, doc_islr.total_neto, 
                            doc_islr.monto_reca, doc_islr.monto_imp, doc_islr.monto_imp2, doc_islr.monto_imp3, doc_islr.saldo, doc_islr.n_control, 
                            doc_islr.num_comprobante, doc_islr.dis_cen, doc_islr.comis1, doc_islr.comis2, doc_islr.comis3, doc_islr.comis4, doc_islr.otros1, 
                            doc_islr.otros2, doc_islr.otros3, doc_islr.porc_desc_glob, doc_islr.monto_desc_glob, doc_islr.porc_reca, doc_islr.porc_imp, 
                            doc_islr.porc_imp2, doc_islr.porc_imp3, doc_islr.salestax, doc_islr.ven_ter, doc_islr.fec_venc, doc_islr.comis5, doc_islr.comis6, 
                            doc_islr.impfis, doc_islr.impfisfac, doc_islr.imp_nro_z, doc_islr.tipo_origen, doc_islr.campo1, doc_islr.campo2, doc_islr.campo3, 
                            doc_islr.campo4, doc_islr.campo5, doc_islr.campo6, doc_islr.campo7, doc_islr.campo8, doc_islr.revisado, doc_islr.trasnfe, 
                            doc_islr.co_sucu_in, doc_islr.co_us_in, socket.gethostname())

                        rengs_islr.append(doc.rowguid)
                        cursor_sec.execute(sp_doc_islr, sp_doc_islr_params)
                    
                    cursor_sec.execute(sp_doc, sp_doc_params)

                # ingresando renglones de pago
                for tp in rengs_tp:

                    sp_tp = """exec pInsertarRenglonesTPCobro @sCob_Num = ?, @sForma_Pag = ?, @sMov_Num_C = ?, @sMov_Num_B = ?, @sNum_Doc = ?, 
                        @bDevuelto = ?, @deMont_Doc = ?, @sCod_Cta = ?, @sCod_Caja = ?, @sdfecha_che = ?, @sCo_Ban = ?, @sCo_Tar = ?, @sCo_Vale = ?, 
                        @iRENG_NUM = ?, @sREVISADO = ?, @sTRASNFE = ?, @sco_sucu_in = ?, @sco_us_in = ?, @sMaquina = ?
                    """
                    sp_tp_params = (tp.cob_num, tp.forma_pag, tp.mov_num_c, tp.mov_num_b, tp.num_doc, tp.devuelto, tp.mont_doc, tp.cod_cta, 
                        tp.cod_caja, tp.fecha_che, tp.co_ban, tp.co_tar, tp.co_vale, tp.reng_num, tp.revisado, tp.trasnfe, tp.co_sucu_in, 
                        tp.co_us_in, socket.gethostname())

                    cursor_sec.execute(sp_tp, sp_tp_params)

                # ingresando renglones iva
                for iva in rengs_iva:
                    r_iva = search_reng_iva(cursor_main, iva)
                    print(r_iva)

                # ingresando renglones islr
                for islr in rengs_islr:
                    r_islr = search_reng_islr(cursor_main, islr)
                    print(r_islr)
                
                # commit de script
                # con_sec.commit()

            except pyodbc.Error as error:
                # error en la ejecucion
                msg.print_error_msg(error)
                con_sec.rollback()
                status = 3
                pass

        cursor_sec.close()
        con_sec.close()

    return status

def update_collect(item, connect_sec):
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
        cob = search_collect(cursor_sec, item.ItemID)

        if cob is None:
            # el cobro no esta en la base secundaria
            status = 2
        else:
            if item.TipoDato == 'string' or item.TipoDato == 'bool':
                query = f"update saCobro set {item.CampoModificado} = '{item.NuevoValor}' where cob_num = '{item.ItemID}'"
            elif item.TipoDato == 'int' or item.TipoDato == 'decimal':
                query = f"update saCobro set {item.CampoModificado} = {item.NuevoValor} where cob_num = '{item.ItemID}'"

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

def delete_collect(item, connect_sec):
    status = 1

    try:
        # intento de conexion a la base secundaria
        con_sec = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}}; SERVER={connect_sec["server"]}; DATABASE={connect_sec["database"]}; UID={connect_sec["username"]}; PWD={connect_sec["password"]}')
        con_sec.autocommit = False
    except:
        # error al conectar a la base secundaria
        status = 0
    else:

        # se inicializa el cursor y se busca el cobro y sus elementos
        cursor_sec = con_sec.cursor()
        cob = search_collect(cursor_sec, item.ItemID)
        cob_doc = search_rengs_doc_collect(cursor_sec, item.ItemID)
        cob_tp = search_rengs_tp_collect(cursor_sec, item.ItemID)

        if cob is None:
            # el cobro no esta en la base secundaria
            status = 2
        else:
            sp_c = f"exec pEliminarCobro @scob_numori = ?, @tsvalidador = ?, @growguid = ?, @sco_us_mo = ?, @smaquina = ?, @sco_sucu_mo = ?"
            sp_c_params = (cob.cob_num, cob.validador, cob.rowguid, 'SYNC', socket.gethostname(), None)

            try:
                # actualizando saldos de caja
                for tp in cob_tp:

                    code = tp.cod_cta if tp.cod_cta is not None else tp.cod_caja
                    tipo_saldo = "EF" if tp.forma_pag == "EF" else "TF"

                    sp_tp = f"""exec pSaldoActualizar @sCodigo = ?,@sForma_Pag = ?, @sTipoSaldo = ?, @deMonto = ?, @bSumarSaldo = 0,
                        @sModulo = N'COBRO', @bPermiteSaldoNegativo = 0
                    """
                    sp_tp_params = (code, tp.forma_pag, tipo_saldo, tp.mont_doc)

                    cursor_sec.execute(sp_tp, sp_tp_params)
                
                movs_c = search_movs_c(cursor_sec, cob.cob_num) # movimientos de caja
                movs_b = search_movs_b(cursor_sec, cob.cob_num) # movimientos de banco

                # eliminando movimientos de caja
                for m in movs_c:
                    sp_mov = f"exec pEliminarMovimientoCaja @smov_numori = ?, @tsvalidador = ?, @growguid = ?, @sco_us_mo = ?, @smaquina = ?, @sco_sucu_mo = ?"
                    sp_mov_params = (m.mov_num, m.validador, m.rowguid, 'SYNC', socket.gethostname(), None)

                    cursor_sec.execute(sp_mov, sp_mov_params)

                # eliminando movimientos de banco
                for m in movs_b:
                    sp_mov = f"exec pEliminarMovimientoBanco @smov_numori = ?, @tsvalidador = ?, @growguid = ?, @sco_us_mo = ?, @smaquina = ?, @sco_sucu_mo = ?"
                    sp_mov_params = (m.mov_num, m.validador, m.rowguid, 'SYNC', socket.gethostname(), None)

                    cursor_sec.execute(sp_mov, sp_mov_params)
                
                # ingresando el cobro
                cursor_sec.execute(sp_c, sp_c_params)

                # commit de script
                con_sec.commit()

            except pyodbc.Error as error:
                # error en la ejecucion
                msg.print_error_msg(error)
                con_sec.rollback()
                status = 3
                pass

    return status

def search_collect (cursor: Cursor, id):
    cursor.execute(f"select * from saCobro where cob_num = '{id}'")
    c = cursor.fetchone()

    return c

def search_rengs_doc_collect (cursor: Cursor, id):
    cursor.execute(f"select * from saCobroDocReng where cob_num = '{id}'")
    rengs = cursor.fetchall()

    return rengs

def search_rengs_tp_collect (cursor: Cursor, id):
    cursor.execute(f"select * from saCobroTPReng where cob_num = '{id}'")
    rengs = cursor.fetchall()

    return rengs

def search_reng_iva (cursor: Cursor, rowguid):
    cursor.execute(f"select * from saCobroRetenIvaReng where rowguid_reng_cob = '{rowguid}'")
    reng = cursor.fetchone()

    return reng

def search_reng_islr (cursor: Cursor, rowguid):
    cursor.execute(f"select * from saCobroRentenReng where rowguid_reng_cob = '{rowguid}'")
    reng = cursor.fetchone()

    return reng

def search_movs_c (cursor: Cursor, cob):
    cursor.execute(f"select * from saMovimientoCaja where origen = 'COB' and doc_num = '{cob}'")
    movs = cursor.fetchall()

    return movs

def search_movs_b (cursor: Cursor, cob):
    cursor.execute(f"select * from saMovimientoBanco where origen = 'COB' and cob_pag = '{cob}'")
    movs = cursor.fetchall()

    return movs

def search_doc_iva (cursor: Cursor, nro_doc):
    cursor.execute(f"select * from saDocumentoVenta where co_tipo_doc = 'IVAN' and nro_doc = '{nro_doc}'")
    doc = cursor.fetchone()

    return doc

def search_doc_islr (cursor: Cursor, nro_doc):
    cursor.execute(f"select * from saDocumentoVenta where co_tipo_doc = 'ISLR' and nro_doc = '{nro_doc}'")
    doc = cursor.fetchone()

    return doc