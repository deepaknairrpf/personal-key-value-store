from datetime import timedelta
import json
import heapq
import datastore.constants as constants
from datetime import datetime
from pytimeparse import parse
from json import JSONEncoder, JSONDecoder
import os
from datastore import utils
from utils import synchronized


class MetaInfo:
    '''An object which represents the meta-info of a key, value pair.

    These objects are stored in the meta file and contains the seek information of each key.
    Additionally, it also contains the ttl of time of creation or updation of each key.
    '''
    class MetaInfoEncoder(JSONEncoder):
        def default(self, o):
            return str(o)

    def __init__(self, key, info_dict, time_format=constants.TIME_FORMAT):
        self.key = key
        self.seek_val = info_dict.get('seek_val')
        self.time_format = time_format
        self.ttl = parse(info_dict.get('ttl')) if info_dict.get('ttl') else None
        self.created_time = datetime.strptime(info_dict.get('created_time'), time_format)

    @classmethod
    def build_meta_info(cls, key, seek_val, ttl=None):
        '''A builder method which returns a meta-info object.

        Args:
            key (str): The key of the object
            seek_val (int): The seek position where the value is stored.
            ttl (int): TTL in seconds as an integer.

        Returns:
            MetaInfo: Corresponding information wrapped as an MetaInfo object.

        '''
        created_time = datetime.now()
        return MetaInfo(key, {
            'created_time': created_time.strftime(constants.TIME_FORMAT),
            'ttl': str(timedelta(seconds=ttl)) if ttl else None,
            'seek_val': seek_val
        })

    def __lt__(self, other):
        '''Comparator implemented to break ties during heapify between two keys
        if they happen to expire at the same time.

        Args:
            other (MetaInfo): The other object with which comparison has to be made.

        Returns:
            bool: Whether this object is considered to be lesser than the given object based off some parameter.

        '''
        return self.created_time < other.created_time

    def expiry_time(self):
        '''Returns the datetime object at which the object expires. If ttl isn't set, it returns None

        Returns:
            datetime: The datetime obj at which this object expires.
            None: If TTL wasn't specified during creation.
        '''

        if not self.ttl:
            return None

        return self.created_time + timedelta(seconds=self.ttl)

    def __str__(self):
        return json.dumps({
            'key': self.key,
            'seek_val': self.seek_val,
            'ttl': self.ttl if self.ttl else None,
            'created_time': self.created_time.strftime(self.time_format)
        })


class FileMeta:
    '''An class which acts as an delegator pattern and exposes API to the DataStore object meanwhile performing all the
    relevant book-keeping.

    This class is responsible for changing the state of the meta information of the file.
    This information is saved as a JSON in a hidden file with meta appended to the name. Therefore, every key value
    store invocation creates two files, one to blindly store the values and another which maps the seek positions
    of those values with their corresponding keys and TTL. An object of this class manages the latter.

    '''
    def __init__(self, meta_dict):
        self.meta_dict = {key: MetaInfo(key, json.loads(value)) for key, value in meta_dict.items()}
        self.expiry_time_heap = []
        self.free_slots = []
        self.preprocess()

    def preprocess(self):
        '''This method initializes the state during creation.

        This method creates a min-heap of the expiration time of all the keys whose TTL was specified during creation.
        This method also initializes a list to keep track of existing free slots which occur due to deletion.
        '''
        self.build_expiry_time_heap()
        sorted_seek_vals = sorted(list(self.meta_dict.values()), key=lambda x: x.seek_val)

        for meta_info_1, meta_info_2 in zip(sorted_seek_vals, sorted_seek_vals[1:]):
            seek_1 = meta_info_1.seek_val
            seek_2 = meta_info_2.seek_val
            diff = (seek_2 - seek_1)

            if diff > constants.VALUE_SIZE:
                self.free_slots += range(seek_1 + constants.VALUE_SIZE, seek_2, constants.VALUE_SIZE)

    def build_expiry_time_heap(self):
        '''Builds a min-heap from expiration time of all keys whose TTL was specified during creation.
        Ties are broken by placing the oldest created key at the top. But ties are extremely rare as
        the precision of datetime objects are 10^-6 of a second. The former was done to simply setup of
        unit tests.

        Returns:
            list: A min-heap where each element is a tuple (expiration_time, meta_info_obj)
        '''
        expiry_times = [
            (meta_info.expiry_time(), meta_info)
            for meta_info in self.meta_dict.values() if meta_info.ttl
        ]
        self.expiry_time_heap = expiry_times
        heapq.heapify(expiry_times)  # An O(n) operation using bottom-up approach.
        return self.expiry_time_heap

    def create(self, key, current_seek, ttl=None):
        '''Creates the meta-info pertaining to a key and maps it to the seek_val where it's value is written in a different
        file.

        This method tries to a find a free slot by looking for a discontinuity at the free_slots list, upon failure
        to find one, it tries to obtain a key which has breached it's expiry time from the min-heap, upon failure
        of which, it considers appending the item to the end of they file.

        Args:
            key (str): Key of the object
            current_seek (int): Current position of the file pointer of the file storing the values.
            ttl (int): TTL of the object in seconds as an integer.

        Returns:
            slot (int): The seek position to which the value should be written. This could either be a free slot or
            the position of another value which has expired or the bottom of the file.
        '''
        slot = current_seek

        if len(self.free_slots):
            slot = self.free_slots.pop(0)

        elif len(self.expiry_time_heap):
            expiry_time, meta_info = self.expiry_time_heap[0]

            if datetime.now() > expiry_time:
                heapq.heappop(self.expiry_time_heap)
                slot = meta_info.seek_val

        meta_info = MetaInfo.build_meta_info(key, slot, ttl=ttl)

        if ttl:
            heapq.heappush(self.expiry_time_heap, (meta_info.expiry_time(), meta_info))

        self.meta_dict[key] = meta_info
        return slot

    def read(self, key):
        '''Returns the meta-info of a key

        Args:
            key (str): Key of the object

        Returns:
            Meta Info: Meta Info corresponding to the key, None if the key is not present.

        '''
        return self.meta_dict.get(key)

    def delete(self, key):
        '''Deletes the key and rebuilds the heap as deletion in a min-heap is as good as rebuilding it from bottom-up

        Args:
            key (str): Key of the object to be deleted.

        Returns:
            Meta Info: Meta Info of the object pertaining to the key, None if the key isn't present.
        '''
        if key not in self.meta_dict:
            return None

        else:
            meta_info = self.meta_dict[key]
            del self.meta_dict[key]
            self.free_slots.append(meta_info.seek_val)
            self.build_expiry_time_heap()
            return meta_info

    def update(self, key, seek_val, ttl=None):
        '''Updates the key with a different MetaInfo object and rebuilds the heap to update the TTL values.

        Args:
            key (str): Key pertaining to the object to be updated.
            seek_val (int): Seek position of the value of the object.
            ttl (int): TTL of the updated object.

        Returns:
            MetaInfo: The updated MetaInfo object
        '''
        meta_info = MetaInfo.build_meta_info(key, seek_val, ttl)
        self.meta_dict[key] = meta_info
        self.build_expiry_time_heap()
        return meta_info


class DataStore:
    '''The API which the clients can consume using an context manager to access the functionalities of the data-store.
    '''

    def __init__(self, filename='personal-data-store-' + str(datetime.now()), storage_dir=constants.STORAGE_DIR):
        self.storage_dir = storage_dir
        utils.create_dir(storage_dir)
        self.filename = self.storage_dir + '/' + filename
        self.file_meta_name = self.get_file_meta_name(filename)
        self.file_handle = None
        self.file_meta = None

    @synchronized
    def create(self, key, value, ttl=None):
        '''API to create a key-value pair and to set an optional TTL value.

        This method obtains the current seek of the file where value objects are stored and delegates the task
        of finding a slot to the FileMeta object, which updates it's hash map and returns the slot to which
        the value obj has to be written.
        As it involves writing to the file based on states, it's a critical section and is run in thread synchrony.

        Args:
            key (str): The key to be created.
            value (dict): The value that has to be stored.
            ttl (int): An optional TTL value in seconds as an integer.

        '''
        current_seek = self.file_handle.tell()
        validation, errors = self._validate(key, value, current_seek)
        if not validation:
            print(errors)
            return

        seek_val = self.file_meta.create(key, current_seek, ttl=ttl)
        self.commit(value, seek_val)
        print('Item wrote: ', value, '\n', 'Position: ', seek_val)

    def read(self, key):
        '''API to read the value of a key from the data store.

        This delegates the read to the FileMeta object which returns a MetaInfo object which contains the
        seek position where value obj resides.

        Args:
            key (str): The key of the object to be read

        Returns:
            str: The value object which was written to the value store.
        '''
        meta_info = self.file_meta.read(key)
        seek_val = meta_info.seek_val
        self.file_handle.seek(seek_val)
        return self.file_handle.read(constants.VALUE_SIZE).lstrip('0')

    @synchronized
    def delete(self, key):
        '''API to delete key-values from the store.

        Args:
            key (str): Key of the object to be deleted.

        '''
        meta_info = self.file_meta.delete(key)

        if meta_info:
            print('Object with ', key, ' deleted')
            return

        print('A value with that key does not exists.')

    @synchronized
    def update(self, key, value, ttl=None):
        '''API to update an existing key with a new value.

        Args:
            key (str): Key of the object
            value (dict): New value to be updated
            ttl (int): Optional TTL in seconds
        '''
        validation, errors = self._validate(key, value, 0)
        if not validation:
            print(errors)
            return
        meta_info = self.file_meta.read(key)
        seek_val = meta_info.seek_val
        self.file_meta.update(key, seek_val, ttl)
        self.commit(value, seek_val)
        print(key, ' updated with ', value)

    def __enter__(self):
        self.file_handle = open(self.filename, 'a+')

        if os.path.exists(self.file_meta_name):
            with open(self.file_meta_name) as meta_file:
                self.file_meta_dict = json.load(meta_file)
        else:
            self.file_meta_dict = {}

        self.file_meta = FileMeta(self.file_meta_dict)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.file_handle.close()

        with open(self.file_meta_name, 'w') as meta_file:
            json.dump(self.file_meta.meta_dict, meta_file, cls=MetaInfo.MetaInfoEncoder)

    def get_file_meta_name(self, filename):
        if filename.startswith('.'):
            meta_filename = filename + '_meta'

        else:
            meta_filename = '.' + filename + '_meta'

        return self.storage_dir + '/' + meta_filename

    def commit(self, value, pos):
        '''Method which commits to a particular postion in a file

        Args:
            value (dict): The object to be written to the file.
            pos (int): The position to which, the value has to be written in the file.

        '''
        self.file_handle.seek(pos)
        self.file_handle.write(json.dumps(value).zfill(constants.VALUE_SIZE))

    def _validate(self, key, value, seek_pos):
        '''A set of validators which prevents the API from being executed if certain conditions aren't met

        Args:
            key (str): The key of the object
            value (dict): The object to be saved
            seek_pos (int): The current position of the file ptr, used to determine the current size of the file.

        Note that, this method is considered private.
        '''
        errors = []
        key_validation = True if utils.utf8len(key) < 32 else False
        value_validation = True if utils.utf8len(json.dumps(value)) < constants.VALUE_SIZE else False
        file_validation = True if seek_pos <= constants.MAX_FILE_SIZE - constants.VALUE_SIZE else False

        if not key_validation:
            errors.append('Max size for key breached. Limit to 32 chars ..')
        if not value_validation:
            errors.append('Max size for value breached. Save objects less than ' + str(constants.VALUE_SIZE))
        if not file_validation:
            errors.append('Max file size reached')

        return key_validation and value_validation and file_validation, errors
