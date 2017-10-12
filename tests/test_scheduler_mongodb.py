#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pytest
import pickle
import mongomock
from helper import HashAndProcessImplement, validate_schduler_implement
from pytq.scheduler_mongodb import MongoDBScheduler


def test():
    class Scheduler(HashAndProcessImplement, MongoDBScheduler):
        collection = mongomock.MongoClient().db.collection

    s = Scheduler()
    validate_schduler_implement(s)

    for doc in s._col.find():
        input_data = int(doc["_id"])
        output_data = pickle.loads(doc["out"])
        assert input_data * 1000 == output_data


if __name__ == "__main__":
    import os

    basename = os.path.basename(__file__)
    pytest.main([basename, "-s", "--tb=native"])
