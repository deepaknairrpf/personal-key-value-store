from datastore.datastore import DataStore

with DataStore(filename='test_file') as data_store:
    data_store.create('deepak', {'name': 'Deepak', 'hobby': 'documentaries'})
    print(data_store.read('deepak'))
