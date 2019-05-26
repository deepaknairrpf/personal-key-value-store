from unittest import TestCase
from datastore.datastore import MetaInfo, FileMeta, DataStore
import datetime
from datetime import timedelta
from pytimeparse import parse
import json
from json import JSONDecodeError
import pytest_freezegun
import datastore.constants as constants
from unittest.mock import MagicMock, patch


class MetaInfoTest(TestCase):

    def test_expiry_time(self):
        created_time = datetime.datetime(2019, 5, 26)
        ttl = str(timedelta(seconds=10))

        meta_info = MetaInfo('test_key', {
            'created_time': created_time,
            'ttl': ttl,
            'seek_val': 12,
        })
        result = meta_info.expiry_time()
        expected_result = created_time + timedelta(seconds=parse(ttl))
        self.assertEqual(result, expected_result)

    def test_expiry_time_returns_None_for_obj_without_ttl(self):
        created_time = datetime.datetime(2019, 5, 26)
        meta_info = MetaInfo('test_key', {
            'created_time': created_time,
            'seek_val': 12,
        })
        self.assertIsNone(meta_info.expiry_time())

    def test_JSON_serializable(self):
        created_time = datetime.datetime(2019, 5, 26)
        ttl = str(timedelta(seconds=10))
        meta_info = MetaInfo('test_key', {
            'created_time': created_time,
            'ttl': ttl,
            'seek_val': 12,
        })
        try:
            json.dumps({'key': meta_info}, cls=MetaInfo.MetaInfoEncoder)

        except TypeError:
            self.fail('MetaInfo object is not JSON serializable.. Check the encoder class')


class FileMetaTest(TestCase):

    @pytest_freezegun.freeze_time('2019-05-26')
    @patch('datastore.datastore.FileMeta.preprocess')
    def test_build_expiry_time_heap_builds_min_heap_appropriately(self, preprocess_mock):
        ttl_seconds = [24, 12, 5, 1, 90]
        created_time = datetime.datetime(2019, 5, 26)
        meta_dict = {}

        for ttl in ttl_seconds:
            meta_dict['test_key_' + str(ttl)] = {
                'created_time': created_time,
                'ttl': str(timedelta(seconds=ttl)),
                'seek_val': 0
            }

        file_meta = FileMeta(meta_dict)
        expiry_time_heap = file_meta.build_expiry_time_heap()
        size = len(expiry_time_heap)

        for idx in range(int((size - 2) / 2) + 1):
            print(
                'Checking whether ', expiry_time_heap[idx], ' is lesser than ',
                expiry_time_heap[2 * idx + 1], ' and ', expiry_time_heap[2 * idx + 2]
            )

            if (
                    expiry_time_heap[idx] > expiry_time_heap[2 * idx + 1]
                    or expiry_time_heap[idx] > expiry_time_heap[2 * idx + 2]
            ):
                self.fail('Heap Invariant not satisfied !')

    def test_preprocess_fills_free_slots_list_appropriately(self):
        seek_vals = [24, 12, 5, 1, 90]
        created_time = datetime.datetime(2019, 5, 26)
        meta_dict = {}

        for seek_val in seek_vals:
            meta_dict['test_key_' + str(seek_val)] = {
                'created_time': created_time,
                'ttl': str(timedelta(seconds=2)),
                'seek_val': seek_val * constants.VALUE_SIZE
            }

        file_meta = FileMeta(meta_dict)
        expected_free_slots = [2, 3, 4] + list(range(6, 12)) + list(range(13, 24)) + list(range(25, 90))
        expected_free_slots = [free_slot * constants.VALUE_SIZE for free_slot in expected_free_slots]
        self.assertEqual(expected_free_slots, file_meta.free_slots)

    @patch('datastore.datastore.FileMeta.preprocess')
    def test_constructor_calls_preprocess_procedure(self, preprocess):
        ttl_seconds = [24, 12, 5, 1, 90]
        created_time = datetime.datetime(2019, 5, 26)
        meta_dict = {}

        for ttl in ttl_seconds:
            meta_dict['test_key_' + str(ttl)] = {
                'created_time': created_time,
                'ttl': str(timedelta(seconds=ttl)),
                'seek_val': 0
            }

        FileMeta(meta_dict)
        preprocess.assert_called()

    @pytest_freezegun.freeze_time('2019-05-26')
    def test_create_returns_current_seek_pos_if_free_slots_and_heap_is_empty(self):
        file_meta = FileMeta({})
        dummy_seek = 12
        dummy_key = 'test_key'
        created_time = datetime.datetime.now()
        seek_val = file_meta.create(dummy_key, dummy_seek)
        self.assertEqual(seek_val, dummy_seek)
        expected_meta_info = {'created_time': created_time, 'ttl': None, 'seek_val': dummy_seek}
        actual_meta_info_obj = file_meta.meta_dict.get(dummy_key)
        actual_meta_info = {
            'created_time': actual_meta_info_obj.created_time,
            'ttl': actual_meta_info_obj.ttl,
            'seek_val': actual_meta_info_obj.seek_val
        }
        self.assertEqual(actual_meta_info, expected_meta_info)

    def test_create_chooses_first_empty_slot_if_available(self):
        file_meta = FileMeta({})
        dummy_free_slot = 14
        dummy_seek = 12
        file_meta.free_slots = [dummy_free_slot]
        dummy_key = 'test_key'
        seek_val = file_meta.create(dummy_key, dummy_seek)
        self.assertEqual(seek_val, dummy_free_slot)
        self.assertEqual(len(file_meta.free_slots), 0)

    @pytest_freezegun.freeze_time('2019-05-26')
    def test_create_does_not_pop_element_with_time_left_to_expire(self):
        ttl_seconds = [24, 12, 5, 1, 90]
        created_time = datetime.datetime(2019, 5, 26)
        meta_dict = {}

        for ttl in ttl_seconds:
            meta_dict['test_key_' + str(ttl)] = {
                'created_time': created_time,
                'ttl': str(timedelta(seconds=ttl)),
                'seek_val': 0
            }

        file_meta = FileMeta(meta_dict)
        seek_val = file_meta.create('test_key', 1025)
        self.assertEqual(seek_val, 1025)

    @pytest_freezegun.freeze_time('2019-05-26')
    def test_create_choose_element_from_the_heap_appropriately(self):
        ttl_seconds = [24, 12, 5, 1, 90]
        seek_vals = [1, 2, 3, 4, 5]

        created_time = datetime.datetime(2019, 5, 24)
        meta_dict = {}

        for ttl, seek_val in zip(ttl_seconds, seek_vals):
            meta_dict['test_key_' + str(ttl)] = {
                'created_time': created_time,
                'ttl': str(timedelta(seconds=ttl)),
                'seek_val': seek_val * constants.VALUE_SIZE
            }

        file_meta = FileMeta(meta_dict)
        seek_val = file_meta.create('test_key', 1025)

        min_element_idx = ttl_seconds.index(min(ttl_seconds))
        min_element_seek_val = seek_vals[min_element_idx]
        self.assertEqual(seek_val, min_element_seek_val * constants.VALUE_SIZE)

    @pytest_freezegun.freeze_time('2019-05-26')
    def test_create_selects_free_slot_even_if_expired_elements_are_present_in_heap(self):
        ttl_seconds = [24, 12, 5, 1, 90]
        seek_vals = [1, 2, 3, 4, 6]  # Note that 5 is missing and hence free !

        created_time = datetime.datetime(2019, 5, 24)
        meta_dict = {}

        for ttl, seek_val in zip(ttl_seconds, seek_vals):
            meta_dict['test_key_' + str(ttl)] = {
                'created_time': created_time,
                'ttl': str(timedelta(seconds=ttl)),
                'seek_val': seek_val * constants.VALUE_SIZE
            }

        file_meta = FileMeta(meta_dict)
        seek_val = file_meta.create('test_key', 1025)

        self.assertEqual(seek_val, 5 * constants.VALUE_SIZE)

    @pytest_freezegun.freeze_time('2019-05-26')
    def test_create_updates_heap_with_expiry_value_based_on_ttl_appropriately(self):
        ttl_seconds = [24, 12, 5, 1, 90]
        seek_vals = [1, 2, 3, 4, 5]

        created_time = datetime.datetime(2019, 5, 24)
        meta_dict = {}

        for ttl, seek_val in zip(ttl_seconds, seek_vals):
            meta_dict['test_key_' + str(ttl)] = {
                'created_time': created_time,
                'ttl': str(timedelta(seconds=ttl)),
                'seek_val': seek_val * constants.VALUE_SIZE
            }

        file_meta = FileMeta(meta_dict)
        seek_val = file_meta.create('test_key', 1025, ttl=3)

        min_element_idx = ttl_seconds.index(min(ttl_seconds))
        min_element_seek_val = seek_vals[min_element_idx]
        self.assertEqual(seek_val, min_element_seek_val * constants.VALUE_SIZE)
        expiry_time_heap = file_meta.expiry_time_heap

        size = len(expiry_time_heap)

        for idx in range(int((size - 2) / 2) + 1):
            print(
                'Checking whether ', expiry_time_heap[idx], ' is lesser than ',
                expiry_time_heap[2 * idx + 1], ' and ', expiry_time_heap[2 * idx + 2]
            )

            if (
                    expiry_time_heap[idx] > expiry_time_heap[2 * idx + 1]
                    or expiry_time_heap[idx] > expiry_time_heap[2 * idx + 2]
            ):
                self.fail('Heap Invariant not satisfied !')

        self.assertEqual(expiry_time_heap[size - 1][1].ttl, 3)

    def test_read(self):
        file_meta = FileMeta({})
        file_meta.create('test_key', 0)
        meta_info = file_meta.read('test_key')
        self.assertIsNotNone(meta_info)

class DataStoreTest(TestCase):
    pass


