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
        print('Ha ocurrido un error estableciendo conexión con la base de datos secundaria')
    elif result == 1:
        print(f'{elem} {id} agregad{l} con éxito')
    elif result == 2:
        print(f"{elem} {id} ya fue sincronizad{l} anteriormente")
    elif result == 3:
        print(f'{elem} {id} no pudo ser agregad{l}')

def print_msg_result_update (elem, id, field, l, result):
    if result == 0:
        print('Ha ocurrido un error estableciendo conexión con la base de datos secundaria')
    elif result == 1:
        print(f'{elem} {id} ({field}) actualizad{l} con éxito')
    elif result == 2:
        print(f"{elem} {id} no se encuentra en la base secundaria, debe ser agregad{l} y/o sincronizad{l} primero")
    elif result == 3:
        print(f'{elem} {id} ({field}) no pudo ser actualizad{l}')

def print_msg_result_delete (elem, id, l, result):
    if result == 0:
        print('Ha ocurrido un error estableciendo conexión con la base de datos secundaria')
    elif result == 1:
        print(f'{elem} {id} eliminad{l} con éxito')
    elif result == 2:
        print(f"{elem} {id} no existe en la base secundaria")
    elif result == 3:
        print(f'{elem} {id} no pudo ser eliminad{l}')

def print_error_msg (error):
    print('-----------------------------------------------------------------')
    print(f'Error => {error}')
    print('-----------------------------------------------------------------')

def print_end_sync():
    print("Finalizado")