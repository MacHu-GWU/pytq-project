#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pytest
from helper import HashAndProcessImplement, validate_schduler_implement
from pytq.scheduler_sql_status_flag import SqlStatusFlagScheduler
import sys

py_ver = "%s.%s" % (sys.version_info.major, sys.version_info.minor)


def test_SqlStatusFlagScheduler():
    class Scheduler(HashAndProcessImplement, SqlStatusFlagScheduler):
        uri = "postgres://dhdshknw:nrrnd3NPc5-CPe_qrV_ngJHbeAmwHf0l@baasu.db.elephantsql.com:5432/dhdshknw"
        table = "sql_status_flag_scheduler_%s" % py_ver
        duplicate_flag = 50
        update_interval = 24 * 3600

    with Scheduler() as s:
        validate_schduler_implement(s, test_multiprocess=False, processes=2)


if __name__ == "__main__":
    import os

    basename = os.path.basename(__file__)
    pytest.main([basename, "-s", "--tb=native"])
