from datetime import datetime

def print_connection_success ():
    print('Conexión exitosa')

def print_connection_error ():
    print('Ha ocurrido un error estableciendo conexión')

def print_no_items_to_insert ():
    print('No hay registros nuevos para ingresar')

def print_no_items_to_update ():
    print('No hay registros nuevos para actualizar')

def print_no_items_to_delete ():
    print('No hay registros nuevos para eliminar')

def print_item_not_found (item, id):
    print(f'{item} {id} no se encuentra en la base principal')

def print_msg_result_insert (elem, id, l, result):
    if result == 0:
        msg = 'Ha ocurrido un error estableciendo conexión con la base de datos secundaria'
    elif result == 1:
        msg = f'{elem} {id} agregad{l} con éxito'
    elif result == 2:
        msg = f"{elem} {id} ya fue sincronizad{l} anteriormente"
    elif result == 3:
        msg = f'{elem} {id} no pudo ser agregad{l}'

    __log_msg(msg)

def print_msg_result_update (elem, id, field, l, result):
    if result == 0:
        msg = 'Ha ocurrido un error estableciendo conexión con la base de datos secundaria'
    elif result == 1:
        msg = f'{elem} {id} ({field}) actualizad{l} con éxito'
    elif result == 2:
        msg = f"{elem} {id} no se encuentra en la base secundaria, debe ser agregad{l} y/o sincronizad{l} primero"
    elif result == 3:
        msg = f'{elem} {id} ({field}) no pudo ser actualizad{l}'

    __log_msg(msg)

def print_msg_result_delete (elem, id, l, result):
    if result == 0:
        msg = 'Ha ocurrido un error estableciendo conexión con la base de datos secundaria'
    elif result == 1:
        msg = f'{elem} {id} eliminad{l} con éxito'
    elif result == 2:
        msg = f"{elem} {id} no existe en la base secundaria"
    elif result == 3:
        msg = f'{elem} {id} no pudo ser eliminad{l}'

    __log_msg(msg)

def print_error_msg (error):
    print('-----------------------------------------------------------------')
    print(f'Error => {error}')
    print('-----------------------------------------------------------------')

    __log_msg(f'Error => {error}')

def print_end_sync():
    print("Finalizado")

def __log_msg(msg):
    now = datetime.now()
    print(msg)
    
    with open('logs.txt', 'a') as f:
        f.write(msg + f" - {now.strftime('%d/%m/%Y %H:%M:%S')}\n")