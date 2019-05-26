from datetime import timedelta
import json
import heapq
import datastore.constants as constants
from datetime import datetime
from pytimeparse import parse
from json import JSONEncoder
import os
from datastore import utils


class Validators:

    @classmethod
    def size_of_key(cls, key):
        pass


class MetaInfo:

    class MetaInfoEncoder(JSONEncoder):
        def default(self, o):
            return str(o)

    def __init__(self, key, info_dict, time_format=constants.TIME_FORMAT):
        self.key = key
        self.seek_val = info_dict.get('seek_val')
        self.time_format = time_format
        self.ttl = parse(info_dict.get('ttl')) if info_dict.get('ttl') else None
        self.created_time = info_dict.get('created_time')

    def __lt__(self, other):
        return self.created_time < other.created_time

    def expiry_time(self):
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
    def __init__(self, meta_dict):
        self.meta_dict = {key: MetaInfo(key, value) for key, value in meta_dict.items()}
        self.expiry_time_heap = []
        self.free_slots = []
        self.preprocess()

    def preprocess(self):
        self.build_expiry_time_heap()
        sorted_seek_vals = sorted(list(self.meta_dict.values()), key=lambda x: x.seek_val)

        for meta_info_1, meta_info_2 in zip(sorted_seek_vals, sorted_seek_vals[1:]):
            seek_1 = meta_info_1.seek_val
            seek_2 = meta_info_2.seek_val
            diff = (seek_2 - seek_1)

            if diff > constants.VALUE_SIZE:
                self.free_slots += range(seek_1 + constants.VALUE_SIZE, seek_2, constants.VALUE_SIZE)

    def build_expiry_time_heap(self):
        expiry_times = [
            (meta_info.expiry_time(), meta_info)
            for meta_info in self.meta_dict.values() if meta_info.ttl
        ]
        self.expiry_time_heap = expiry_times
        heapq.heapify(expiry_times)  # An O(n) operation using bottom-up approach.
        return self.expiry_time_heap

    def create(self, key, current_seek, ttl=None):
        slot = current_seek
        now = datetime.now()

        if len(self.free_slots):
            slot = self.free_slots.pop(0)

        elif len(self.expiry_time_heap):
            expiry_time, meta_info = self.expiry_time_heap[0]

            if datetime.now() > expiry_time:
                heapq.heappop(self.expiry_time_heap)
                slot = meta_info.seek_val

        meta_info = MetaInfo(key, {
            'created_time': now,
            'ttl': str(timedelta(seconds=ttl)) if ttl else None,
            'seek_val': slot,
        })

        if ttl:
            heapq.heappush(self.expiry_time_heap, (meta_info.expiry_time(), meta_info))

        self.meta_dict[key] = meta_info
        return slot

    def read(self, key):
        return self.meta_dict.get(key)


class DataStore:

    def __init__(self, filename='personal-data-store-' + str(datetime.now()), storage_dir=constants.STORAGE_DIR):
        self.storage_dir = storage_dir
        utils.create_dir(storage_dir)
        self.filename = self.storage_dir + '/' + filename
        self.file_meta_name = self.get_file_meta_name(filename)
        self.file_meta_dict = None
        self.file_handle = None
        self.file_meta = None

    def create(self, key, value):
        current_seek = self.file_handle.tell()
        ttl = value.get('ttl')
        seek_val = self.file_meta.create(key, current_seek, ttl=ttl)
        self.file_handle.seek(seek_val)
        self.file_handle.write(json.dumps(value).zfill(constants.VALUE_SIZE))
        print(value, '\n', 'Seek pos: ', seek_val, '\n', 'TTL: ', ttl)

    def read(self, key):
        meta_info = self.file_meta.read(key)
        seek_val = meta_info.seek_val
        self.file_handle.seek(seek_val)
        return self.file_handle.read(constants.VALUE_SIZE).lstrip('0')

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
            json.dump(self.file_meta_dict, meta_file, cls=MetaInfo.MetaInfoEncoder)

    def get_file_meta_name(self, filename):
        if filename.startswith('.'):
            meta_filename = filename + '_meta'

        else:
            meta_filename = '.' + filename + '_meta'

        return self.storage_dir + '/' + meta_filename
