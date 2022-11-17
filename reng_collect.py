import pyodbc
import socket
import messages as msg
from pyodbc import Cursor

def insert_reng_tp_collect (item, connect_sec):
    status = 1

    try:
        # intento de conexion a la base secundaria
        con_sec = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}}; SERVER={connect_sec["server"]}; DATABASE={connect_sec["database"]}; UID={connect_sec["username"]}; PWD={connect_sec["password"]}')
        con_sec.autocommit = False
    except:
        # error al conectar a la base secundaria
        status = 0
    else:

        # se inicializa el cursor y se busca el renglon
        cursor_sec = con_sec.cursor()
        r = search_reng_tp_collect(cursor_sec, item.cob_num, item.reng_num)

        if r is not None:
            # el renglon ya esta en la base secundaria
            status = 2
        else:
            sp_item = f"""exec pInsertarRenglonesTPCobro @sCob_Num = ?, @sForma_Pag = ?, @sMov_Num_C = ?, @sMov_Num_B = ?, @sNum_Doc = ?, 
                @bDevuelto = ?, @deMont_Doc = ?, @sCod_Cta = ?, @sCod_Caja = ?, @sdfecha_che = ?, @sCo_Ban = ?, @sCo_Tar = ?, @sCo_Vale = ?, 
                @iRENG_NUM = ?, @sREVISADO = ?, @sTRASNFE = ?, @sco_sucu_in = ?, @sco_us_in = ?, @sMaquina = ?
            """
            sp_item_params = (item.cob_num, item.forma_pag, item.mov_num_c, item.mov_num_b, item.num_doc, item.devuelto, item.mont_doc, 
                item.cod_cta, item.cod_caja, item.fecha_che, item.co_ban, item.co_tar, item.co_vale, item.reng_num, item.revisado, item.trasnfe, 
                item.co_sucu_in, 'SYNC', socket.gethostname())

            code = item.cod_cta if item.cod_cta is not None else item.cod_caja
            tipo_saldo = "EF" if item.forma_pag == "EF" else "TF"

            sp_tp = f"""exec pSaldoActualizar @sCodigo = ?,@sForma_Pag = ?, @sTipoSaldo = ?, @deMonto = ?, @bSumarSaldo = 1,
                @sModulo = N'COBRO', @bPermiteSaldoNegativo = 0
            """
            sp_tp_params = (code, item.forma_pag, tipo_saldo, item.mont_doc)

            try:
                # ejecucion de script
                cursor_sec.execute(sp_tp, sp_tp_params)
                cursor_sec.execute(sp_item, sp_item_params)
                con_sec.commit()
            except pyodbc.Error as error:
                # error en la ejecucion
                msg.print_error_msg(error)
                status = 3
                pass

        cursor_sec.close()
        con_sec.close()

    return status

def update_reng_doc_collect (item, cob, reng, connect_sec):
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
        r = search_reng_doc_collect(cursor_sec, cob, reng)

        if r is None:
            # el renglon no esta en la base secundaria
            status = 2
        else:

            if item.NuevoValor is None:
                query = f"""update saCobroDocReng set {item.CampoModificado} = NULL, co_us_mo = 'SYNC'
                            where cob_num = '{cob}' and reng_num = '{reng}'"""
            else:
                if item.TipoDato == 'string' or item.TipoDato == 'bool':
                    query = f"""update saCobroDocReng set {item.CampoModificado} = '{item.NuevoValor}', co_us_mo = 'SYNC'
                                where cob_num = '{cob}' and reng_num = '{reng}'"""
                elif item.TipoDato == 'int' or item.TipoDato == 'decimal':
                    query = f"""update saCobroDocReng set {item.CampoModificado} = {item.NuevoValor}, co_us_mo = 'SYNC'
                                where cob_num = '{cob}' and reng_num = '{reng}'"""

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

def update_reng_tp_collect (item, cob, reng, connect_sec):
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
        r = search_reng_tp_collect(cursor_sec, cob, reng)

        if r is None:
            # el renglon no esta en la base secundaria
            status = 2
        else:

            if item.NuevoValor is None:
                query = f"""update saCobroTPReng set {item.CampoModificado} = NULL, co_us_mo = 'SYNC'
                            where cob_num = '{cob}' and reng_num = '{reng}'"""
            else:
                if item.TipoDato == 'string' or item.TipoDato == 'bool':
                    query = f"""update saCobroTPReng set {item.CampoModificado} = '{item.NuevoValor}', co_us_mo = 'SYNC'
                                where cob_num = '{cob}' and reng_num = '{reng}'"""
                elif item.TipoDato == 'int' or item.TipoDato == 'decimal':
                    query = f"""update saCobroTPReng set {item.CampoModificado} = {item.NuevoValor}, co_us_mo = 'SYNC'
                                where cob_num = '{cob}' and reng_num = '{reng}'"""

            try:
                diff = float(item.AntiguoValor) - float(item.NuevoValor)
                sign = 1 if diff < 0 else 0
                code = r.cod_cta if r.cod_cta is not None else r.cod_caja
                tipo_saldo = "EF" if r.forma_pag == "EF" else "TF"

                if diff < 0:
                    diff *= -1

                sp_tp = f"""exec pSaldoActualizar @sCodigo = ?,@sForma_Pag = ?, @sTipoSaldo = ?, @deMonto = ?, @bSumarSaldo = ?,
                    @sModulo = N'COBRO', @bPermiteSaldoNegativo = 0
                """
                sp_tp_params = (code, r.forma_pag, tipo_saldo, diff, sign)

                # ejecucion de script
                cursor_sec.execute(sp_tp, sp_tp_params)
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

def delete_reng_tp_collect (cob, reng, connect_sec):
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
        r = search_reng_tp_collect(cursor_sec, cob, reng)

        if r is None:
            # el renglon no esta en la base secundaria
            status = 2
        else:
            sp_r = f"exec pEliminarRenglonesTPCobro @sCob_NumOri = ?, @iRENG_NUMOri = ?, @growguid = ?, @sCo_Us_Mo = ?, @sMaquina = ?, @sCo_Sucu_Mo = ?"
            sp_r_params = (r.cob_num, r.reng_num, r.rowguid, 'SYNC', socket.gethostname(), None)

            try:
                
                code = r.cod_cta if r.cod_cta is not None else r.cod_caja
                tipo_saldo = "EF" if r.forma_pag == "EF" else "TF"

                sp_tp = f"""exec pSaldoActualizar @sCodigo = ?,@sForma_Pag = ?, @sTipoSaldo = ?, @deMonto = ?, @bSumarSaldo = 0,
                    @sModulo = N'COBRO', @bPermiteSaldoNegativo = 0
                """
                sp_tp_params = (code, r.forma_pag, tipo_saldo, r.mont_doc)

                cursor_sec.execute(sp_tp, sp_tp_params)
                cursor_sec.execute(sp_r, sp_r_params)

                cursor_sec.commit()

            except pyodbc.Error as error:
                # error en la ejecucion
                msg.print_error_msg(error)
                status = 3
                pass

    return status

def search_reng_doc_collect (cursor: Cursor, id, reng):
    cursor.execute(f"select * from saCobroDocReng where cob_num = '{id}' and reng_num = {reng}")
    new_reng = cursor.fetchone()

    return new_reng

def search_reng_tp_collect (cursor: Cursor, id, reng):
    cursor.execute(f"select * from saCobroTPReng where cob_num = '{id}' and reng_num = {reng}")
    new_reng = cursor.fetchone()

    return new_reng

def search_reng_adel_collect (cursor: Cursor, num_doc):
    cursor.execute(f"select * from saCobroDocReng where co_tipo_doc = 'ADEL' and nro_doc = '{num_doc}'")
    adel = cursor.fetchone()

    return adel