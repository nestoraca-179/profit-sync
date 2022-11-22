import pyodbc
import socket
import messages as msg
from pyodbc import Cursor

def insert_client (c, connect_sec):
    status = 1

    try:
        # intento de conexion a la base secundaria
        con_sec = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}}; SERVER={connect_sec["server"]}; DATABASE={connect_sec["database"]}; UID={connect_sec["username"]}; PWD={connect_sec["password"]}')
    except:
        # error al conectar a la base secundaria
        status = 0
    else:

        # se inicializa el cursor y se busca el cliente
        cursor_sec = con_sec.cursor()
        cli = search_client(cursor_sec, c.co_cli)

        if cli is not None:
            # el cliente ya esta en la base secundaria
            status = 2
        else:
            sp = f"""exec pInsertarCliente @sco_cli = ?, @slogin = ?, @spassword = ?, @ssalestax = ?, @scli_des = ?, @sco_seg = ?, 
                @sco_zon = ?, @sco_ven = ?, @sestado = ?, @binactivo = ?, @bvalido = ?, @bsincredito = ?, @blunes = ?, @bmartes = ?, @bmiercoles = ?, 
                @bjueves = ?, @bviernes = ?, @bsabado = ?, @bdomingo = ?, @sdirec1 = ?, @sdirec2 = ?, @sdir_ent2 = ?, @shorar_caja = ?, @sfrecu_vist = ?, 
                @stelefonos = ?, @sfax = ?, @srespons = ?, @sdfecha_reg = ?, @stip_cli = ?, @sserialp = ?, @ipuntaje = ?, @iid = ?, @demont_cre = ?, 
                @sco_mone = ?, @scond_pag = ?, @iplaz_pag = ?, @dedesc_ppago = ?, @dedesc_glob = ?, @srif = ?, @bcontrib = ?, @sdis_cen = ?, @snit = ?, 
                @semail = ?, @sco_cta_ingr_egr = ?, @scomentario = ?, @scampo1 = ?, @scampo2 = ?, @scampo3 = ?, @scampo4 = ?, @scampo5 = ?, @scampo6 = ?, 
                @scampo7 = ?, @scampo8 = ?, @sco_us_in = ?, @smaquina = ?, @srevisado = ?, @strasnfe = ?, @sco_sucu_in = ?, @bjuridico = ?, 
                @itipo_adi = ?, @smatriz = ?, @sco_tab = ?, @stipo_per = ?, @sco_pais = ?, @sciudad = ?, @szip = ?, @swebsite = ?, @bcontribu_e = ?, 
                @brete_regis_doc = ?, @deporc_esp = ?, @semail_alterno = ?
            """
            params = (c.co_cli, c.login, c.password, c.salestax, c.cli_des, c.co_seg, c.co_zon, c.co_ven, c.estado, c.inactivo, c.valido, c.sincredito, 
                c.lunes, c.martes, c.miercoles, c.jueves, c.viernes, c.sabado, c.domingo, c.direc1, c.direc2, c.dir_ent2, c.horar_caja, c.frecu_vist, 
                c.telefonos, c.fax, c.respons, c.fecha_reg, c.tip_cli, c.serialp, c.puntaje, c.Id, c.mont_cre, c.co_mone, c.cond_pag, c.plaz_pag, 
                c.desc_ppago, c.desc_glob, c.rif, c.contrib, c.dis_cen, c.nit, c.email, c.co_cta_ingr_egr, c.comentario, c.campo1, c.campo2, c.campo3, 
                c.campo4, c.campo5, c.campo6, c.campo7, c.campo8, 'SYNC', socket.gethostname(), c.revisado, c.trasnfe, c.co_sucu_in, c.juridico, 
                c.tipo_adi, c.matriz, c.co_tab, c.tipo_per, c.co_pais, c.ciudad, c.zip, c.website, c.contribu_e, c.rete_regis_doc, c.porc_esp, 
                c.email_alterno)

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

def update_client (item, connect_sec):
    status = 1

    try:
        # intento de conexion a la base secundaria
        con_sec = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}}; SERVER={connect_sec["server"]}; DATABASE={connect_sec["database"]}; UID={connect_sec["username"]}; PWD={connect_sec["password"]}')
    except:
        # error al conectar a la base secundaria
        status = 0
    else:

        # se inicializa el cursor y se busca el cliente
        cursor_sec = con_sec.cursor()
        c = search_client(cursor_sec, item.ItemID)

        if c is None:
            # el cliente no esta en la base secundaria
            status = 2
        else:

            if item.NuevoValor is None:
                query = f"update saCliente set {item.CampoModificado} = NULL, co_us_mo = 'SYNC' where co_cli = '{item.ItemID}'"
            else:
                if item.TipoDato == 'string' or item.TipoDato == 'bool':
                    query = f"update saCliente set {item.CampoModificado} = '{item.NuevoValor}', co_us_mo = 'SYNC' where co_cli = '{item.ItemID}'"
                elif item.TipoDato == 'int' or item.TipoDato == 'decimal':
                    query = f"update saCliente set {item.CampoModificado} = {item.NuevoValor}, co_us_mo = 'SYNC' where co_cli = '{item.ItemID}'"

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

def delete_client (item, connect_sec):
    status = 1

    try:
        # intento de conexion a la base secundaria
        con_sec = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}}; SERVER={connect_sec["server"]}; DATABASE={connect_sec["database"]}; UID={connect_sec["username"]}; PWD={connect_sec["password"]}')
    except:
        # error al conectar a la base secundaria
        status = 0
    else:

        # se inicializa el cursor y se busca el cliente
        cursor_sec = con_sec.cursor()
        c = search_client(cursor_sec, item.ItemID)

        if c is None:
            # el cliente no esta en la base secundaria
            status = 2
        else:

            sp = f"exec pEliminarCliente @sco_cliori = ?, @tsvalidador = ?, @smaquina = ?, @sco_us_mo = ?, @sco_sucu_mo = ?, @growguid = ?"
            params = (c.co_cli, c.validador, socket.gethostname(), 'SYNC', c.co_sucu_mo, c.rowguid)

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

def search_client (cursor: Cursor, id):
    cursor.execute(f"select * from saCliente where co_cli = '{id}'")
    c = cursor.fetchone()

    return c