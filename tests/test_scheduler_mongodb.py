#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pytest
import mongomock
from helper import HashAndProcessImplement, validate_schduler_implement
from pytq.scheduler_mongodb import MongoDBScheduler


def test():
    class Scheduler(HashAndProcessImplement, MongoDBScheduler):
        collection = mongomock.MongoClient().db.collection

    s = Scheduler()
    validate_schduler_implement(s)


if __name__ == "__main__":
    import os

    basename = os.path.basename(__file__)
    pytest.main([basename, "-s", "--tb=native"])
