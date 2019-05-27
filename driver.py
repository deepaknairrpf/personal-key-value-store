from datastore.datastore import DataStore
import threading


def simulate_apis():
    with DataStore(filename='test_file') as data_store:
        data_store.create('deepak', {'name': 'Deepak', 'hobby': 'documentaries'})

        print('Reading key deepak')
        print(data_store.read('deepak'))

        data_store.update('deepak', {'name': 'Deepak Nair', 'hobby': 'books'})
        data_store.delete('deepak')


thread1 = threading.Thread(target=simulate_apis)
thread2 = threading.Thread(target=simulate_apis)

thread1.start()
thread2.start()

thread1.join()
thread2.join()