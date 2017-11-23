#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pytest
from helper import HashAndProcessImplement, validate_schduler_implement
from pytq.scheduler_sql import SqlScheduler
import sys

py_ver = "%s.%s" % (sys.version_info.major, sys.version_info.minor)


def test():
    class Scheduler(HashAndProcessImplement, SqlScheduler):
        uri = "postgres://ujvdiudg:v-40cPbBVbUYWmpjP1ZEBTd2uf7yAA36@baasu.db.elephantsql.com:5432/ujvdiudg"

    with Scheduler(table="sql_scheduler_%s" % py_ver) as s:
        validate_schduler_implement(s, test_multiprocess=False, processes=2)


if __name__ == "__main__":
    import os

    basename = os.path.basename(__file__)
    pytest.main([basename, "-s", "--tb=native"])
