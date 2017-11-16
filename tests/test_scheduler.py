#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pytest
from pytest import raises, approx
from helper import HashAndProcessImplement, validate_schduler_implement
from pytq.scheduler import BaseScheduler


def test_wrong_implement():
    s = BaseScheduler()
    with raises(Exception):
        validate_schduler_implement(s)


class MyScheduler(HashAndProcessImplement, BaseScheduler):
    def __init__(self):
        self._data = dict()
        super(MyScheduler, self).__init__()

    def user_post_process(self, task):
        self._data[task.id] = task.output_data

    def user_is_duplicate(self, task):
        return task.id in self._data

    def __len__(self):
        return len(self._data)

    def __iter__(self):
        return iter(self._data)

    def clear_all(self):
        self._data.clear()

    def get(self, id):
        return self._data[id]


def test_basic_implement():
    s = MyScheduler()
    validate_schduler_implement(s)


if __name__ == "__main__":
    import os

    basename = os.path.basename(__file__)
    pytest.main([basename, "-s", "--tb=native"])
