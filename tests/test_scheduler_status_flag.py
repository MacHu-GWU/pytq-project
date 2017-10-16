#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pytest
import pickle
import mongomock
from datetime import datetime
from helper import HashAndProcessImplement, validate_schduler_implement
from pytq.scheduler_status_flag import StatusFlagScheduler


def test_StatusFlagScheduler():
    class Scheduler(HashAndProcessImplement, StatusFlagScheduler):
        collection = mongomock.MongoClient().db.test_StatusFlagScheduler
        duplicate_flag = 8
        update_interval = 24 * 3600

    s = Scheduler()
    validate_schduler_implement(s)

    for doc in s._col.find():
        input_data = int(doc["_id"])
        output_data = pickle.loads(doc["out"])
        assert input_data * 1000 == output_data
        assert doc[s.status_key] == 8
        assert isinstance(doc[s.edit_at_key], datetime)


if __name__ == "__main__":
    import os

    basename = os.path.basename(__file__)
    pytest.main([basename, "-s", "--tb=native"])
