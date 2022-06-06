# modulos
import client
import invoice
import reng_invoice
import sale_doc
import collect
import messages as msg
import sync_manager as sm

# librerias
import pyodbc
import time

# variables
items_total_inv = []
items_saldo_doc = []

# conexiones
connect_main = {
    "server": "AC-10\SQLS2014SE",
    "database": "PROFIT_2",
    "username": "sa",
    "password": "Soporte123456"
}

connect_sec = {
    "server": "AC-10\SQLS2014SE",
    "database": "PROFIT_1",
    "username": "sa",
    "password": "Soporte123456"
}

connect_sync = {
    "server": "AC-10\SQLS2014SE",
    "database": "ProfitSync",
    "username": "sa",
    "password": "Soporte123456"
}

# MAIN
try:
    # iniciando conexiones a sync y main
    con_sync = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}}; SERVER={connect_sync["server"]}; DATABASE={connect_sync["database"]}; UID={connect_sync["username"]}; PWD={connect_sync["password"]}')
    con_main = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}}; SERVER={connect_main["server"]}; DATABASE={connect_main["database"]}; UID={connect_main["username"]}; PWD={connect_main["password"]}')
    msg.print_connection_success()
    # conexion exitosa
except:
    msg.print_connection_error()
    pass
else:
    # cursores para las DB
    cursor_sync = con_sync.cursor()
    cursor_main = con_main.cursor()

    # consultando los items para DELETE
    cursor_sync.execute('select * from ItemsEliminar where Sincronizado = 0 order by Fecha')
    itemsDel = cursor_sync.fetchall()

    if len(itemsDel) > 0:
        for item in itemsDel:

            if item.Tipo == "CLI":
                
                result = client.delete_client(item, connect_sec)
                msg.print_msg_result_delete('Cliente', item.ItemID, 'o', result)

                if result == 1 or result == 2:
                    sm.update_item(cursor_sync, 'ItemsEliminar', item.ID)

            elif item.Tipo == "FV":

                result = invoice.delete_invoice(item, connect_sec)
                msg.print_msg_result_delete('Factura', item.ItemID, 'a', result)

                if result == 1 or result == 2:
                    sm.update_item(cursor_sync, 'ItemsEliminar', item.ID)

            elif item.Tipo == "FVR":

                index = str.rfind(item.ItemID, '-')
                fact = item.ItemID[0:index]
                reng = item.ItemID[index + 1:]

                result = reng_invoice.delete_reng_invoice(fact, reng, connect_sec)
                msg.print_msg_result_delete(f'Renglon N° {reng} de la factura', fact, 'o', result)

                if result == 1 or result == 2:
                    sm.update_item(cursor_sync, 'ItemsEliminar', item.ID)

            elif item.Tipo == "COB":

                result = collect.delete_collect(item, connect_sec)
                msg.print_msg_result_delete('Cobro', item.ItemID, 'o', result)

                if result == 1 or result == 2:
                    sm.update_item(cursor_sync, 'ItemsEliminar', item.ID)
    else:
        msg.print_no_items_to_delete()

    # consultando los items para UPDATE
    cursor_sync.execute('select * from VIEW_ITEMS_MODIFICAR where Sincronizado = 0 order by Fecha')
    itemsMod = cursor_sync.fetchall()

    if len(itemsMod) > 0:
        for item in itemsMod:

            if item.Tipo == "CLI":

                c = client.search_client(cursor_main, item.ItemID)

                if c is None:
                    msg.print_item_not_found('El cliente', item.ItemID)
                    sm.delete_item(cursor_sync, 'ItemsModificar', item.ID)
                else:

                    result = client.update_client(item, connect_sec)
                    msg.print_msg_result_update('Cliente', item.ItemID, item.CampoModificado, 'o', result)

                    if result == 1:
                        sm.update_item(cursor_sync, 'ItemsModificar', item.ID)

            elif item.Tipo == "FV":

                i = invoice.search_invoice(cursor_main, item.ItemID)

                if i is None:
                    msg.print_item_not_found('La factura', item.ItemID)
                    sm.delete_item(cursor_sync, 'ItemsModificar', item.ID)
                else:

                    if item.CampoModificado == "total_neto" and item.AntiguoValor > item.NuevoValor:
                        items_total_inv.append(item)
                    else:

                        result = invoice.update_invoice(item, connect_sec)
                        msg.print_msg_result_update('Factura', item.ItemID, item.CampoModificado, 'a', result)

                        if result == 1:
                            sm.update_item(cursor_sync, 'ItemsModificar', item.ID)            

            elif item.Tipo == "FVR":

                index = str.rfind(item.ItemID, '-')
                fact = item.ItemID[0:index]
                reng = item.ItemID[index + 1:]

                i = invoice.search_invoice(cursor_main, fact)

                if i is None:
                    msg.print_item_not_found('La factura', fact)
                    sm.delete_item(cursor_sync, 'ItemsModificar', item.ID)
                else:
                    
                    result = reng_invoice.update_reng_invoice(item, fact, reng, connect_sec)
                    msg.print_msg_result_update(f'Renglon N° {reng} de la factura', fact, item.CampoModificado, 'o', result)

                    if result == 1:
                        sm.update_item(cursor_sync, 'ItemsModificar', item.ID)
    
            elif item.Tipo == "DVF":

                d = sale_doc.search_sale_doc(cursor_main, item.ItemID)

                if d is None:
                    msg.print_item_not_found('Documento de venta de la factura', item.ItemID)
                    sm.delete_item(cursor_sync, 'ItemsModificar', item.ID)
                else:

                    if item.CampoModificado == "saldo" and item.AntiguoValor < item.NuevoValor:
                        items_saldo_doc.append(item)
                    else:

                        result = sale_doc.update_sale_doc(item, connect_sec)
                        msg.print_msg_result_update('Documento de venta de la factura', item.ItemID, item.CampoModificado, 'o', result)

                        if result == 1:
                            sm.update_item(cursor_sync, 'ItemsModificar', item.ID)

            elif item.Tipo == "COB":

                c = collect.search_collect(cursor_main, item.ItemID)

                if c is None:
                    msg.print_item_not_found('Cobro', item.ItemID)
                    sm.delete_item(cursor_sync, 'ItemsModificar', item.ID)
                else:

                    result = collect.update_collect(item, connect_sec)
                    msg.print_msg_result_update('Cobro', item.ItemID, item.CampoModificado, 'o', result)

                    if result == 1:
                        sm.update_item(cursor_sync, 'ItemsModificar', item.ID)
        
        # modificando campo total neto en tabla factura de venta
        for total_inv in items_total_inv:
        
            result = sale_doc.update_sale_doc(total_inv, connect_sec)
            msg.print_msg_result_update('Factura', total_inv.ItemID, total_inv.CampoModificado, 'a', result)

            if result == 1:
                sm.update_item(cursor_sync, 'ItemsModificar', total_inv.ID)
        
        # modificando campo saldo en tabla documento de venta
        for saldo_doc in items_saldo_doc:
        
            result = sale_doc.update_sale_doc(saldo_doc, connect_sec)
            msg.print_msg_result_update('Documento de venta de la factura', saldo_doc.ItemID, saldo_doc.CampoModificado, 'o', result)

            if result == 1:
                sm.update_item(cursor_sync, 'ItemsModificar', saldo_doc.ID)
    
    else:
        msg.print_no_items_to_update()
    
    # consultando los items para INSERT
    cursor_sync.execute('select * from ItemsAgregar where Sincronizado = 0 order by Fecha')
    itemsAdd = cursor_sync.fetchall()

    if len(itemsAdd) > 0:
        for item in itemsAdd:

            if item.Tipo == "CLI":
                
                # busca el cliente en la base principal
                c = client.search_client(cursor_main, item.ItemID)

                if c is None: # error si el cliente no esta en la base principal
                    msg.print_item_not_found('El cliente', item.ItemID)
                    sm.delete_item(cursor_sync, 'ItemsAgregar', item.ID)
                else:

                    # se intenta registrar el cliente
                    result = client.insert_client(c, connect_sec)
                    msg.print_msg_result_insert('Cliente', item.ItemID, 'o', result)

                    # se actualiza el registro en profit sync
                    if result == 1 or result == 2:
                        sm.update_item(cursor_sync, 'ItemsAgregar', item.ID)

            elif item.Tipo == "FV":

                # busca la factura en la base principal
                i = invoice.search_invoice(cursor_main, item.ItemID)

                if i is None: # error si la factura no esta en la base principal
                    msg.print_item_not_found('La factura', item.ItemID)
                    sm.delete_item(cursor_sync, 'ItemsAgregar', item.ID)
                else:

                    items = reng_invoice.search_all_invoice_items(cursor_main, item.ItemID) # items de la factura
                    doc = sale_doc.search_sale_doc(cursor_main, item.ItemID) # documento de venta de la factura

                    # se intenta registrar la factura
                    result = invoice.insert_invoice(i, items, doc, connect_sec)
                    msg.print_msg_result_insert('Factura', item.ItemID, 'a', result)

                    # se actualiza el registro en profit sync
                    if result == 1 or result == 2:
                        sm.update_item(cursor_sync, 'ItemsAgregar', item.ID)

            elif item.Tipo == "FVR":

                # busca nro de la factura y nro de renglon
                index = str.rfind(item.ItemID, '-')
                fact = item.ItemID[0:index]
                reng = item.ItemID[index + 1:]

                # busca la factura y el renglon en la base principal
                i = invoice.search_invoice(cursor_main, fact)
                new_reng = reng_invoice.search_reng_invoice(cursor_main, fact, reng)

                if i is None: # error si la factura no esta en la base principal
                    msg.print_item_not_found('La factura', fact)
                    sm.delete_item(cursor_sync, 'ItemsAgregar', item.ID)
                elif new_reng is None: # error si el renglon no esta en la base principal
                    msg.print_item_not_found(f'El renglón {reng} de la factura', fact)
                    sm.delete_item(cursor_sync, 'ItemsAgregar', item.ID)
                else:

                    # se intenta registrar el renglon
                    result = reng_invoice.insert_reng_invoice(new_reng, connect_sec)
                    msg.print_msg_result_insert(f'Renglon N° {reng} de la factura', fact, 'o', result)

                    # se actualiza el registro en profit sync
                    if result == 1 or result == 2:
                        sm.update_item(cursor_sync, 'ItemsAgregar', item.ID)
            
            elif item.Tipo == "COB":

                # busca el cobro en la base principal
                cob = collect.search_collect(cursor_main, item.ItemID)

                if cob is None: # error si el cobro no esta en la base principal
                    msg.print_item_not_found('El cobro', item.ItemID)
                    sm.delete_item(cursor_sync, 'ItemsAgregar', item.ID)
                else:

                    rengs_doc = collect.search_rengs_doc_collect(cursor_main, item.ItemID) # items doc del cobro
                    rengs_tp = collect.search_rengs_tp_collect(cursor_main, item.ItemID) # items pago del cobro

                    # se intenta registrar el cobro
                    result = collect.insert_collect(cob, rengs_doc, rengs_tp, cursor_main, connect_sec)
                    msg.print_msg_result_insert('Cobro', item.ItemID, 'o', result)

                    # se actualiza el registro en profit sync
                    if result == 1 or result == 2:
                        sm.update_item(cursor_sync, 'ItemsAgregar', item.ID)
    else:
        msg.print_no_items_to_insert()
    
    # cerrando los cursores
    cursor_main.close()
    cursor_sync.close()

finally:
    # fin de la sincronizacion
    con_main.close()
    con_sync.close()

    time.sleep(3)
    print("Finalizado")