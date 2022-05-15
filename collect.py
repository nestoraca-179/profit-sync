from pyodbc import Cursor
import pyodbc
import socket
import messages as msg

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
                # actualizando saldos de caja
                for tp in rengs_tp:

                    code = tp.cod_cta if tp.cod_cta is not None else tp.cod_caja
                    tipo_saldo = "EF" if tp.forma_pag == "EF" else "TF"

                    sp_tp = f"""exec pSaldoActualizar @sCodigo = ?,@sForma_Pag = ?, @sTipoSaldo = ?, @deMonto = ?, @bSumarSaldo = 1,
                        @sModulo = N'COBRO', @bPermiteSaldoNegativo = 0
                    """
                    sp_tp_params = (code, tp.forma_pag, tipo_saldo, tp.mont_doc)

                    cursor_sec.execute(sp_tp, sp_tp_params)

                movs_c = search_movs_c(cursor_main, c.cob_num) # caja
                movs_b = search_movs_b(cursor_main, c.cob_num) # bancos

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

def search_movs_c (cursor: Cursor, cob):
    cursor.execute(f"select * from saMovimientoCaja where origen = 'COB' and doc_num = '{cob}'")
    movs = cursor.fetchall()

    return movs

def search_movs_b (cursor: Cursor, cob):
    cursor.execute(f"select * from saMovimientoBanco where origen = 'COB' and cob_pag = '{cob}'")
    movs = cursor.fetchall()

    return movs