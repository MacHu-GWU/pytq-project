#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pytest
from helper import HashAndProcessImplement, validate_schduler_implement
from pytq.scheduler_sqlitedict import SqliteDictScheduler


def test():
    class Scheduler(HashAndProcessImplement, SqliteDictScheduler):
        user_db_path = ":memory:"

    s = Scheduler()
    validate_schduler_implement(s)


if __name__ == "__main__":
    import os

    basename = os.path.basename(__file__)
    pytest.main([basename, "-s", "--tb=native"])
