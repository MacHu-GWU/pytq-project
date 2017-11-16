#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pytest
from pytest import raises, approx


def test():
    import pytq
    pytq.BaseScheduler
    pytq.BaseDBTableBackedScheduler
    pytq.SqliteDictScheduler
    pytq.SqlScheduler
    pytq.SqlStatusFlagScheduler
    pytq.MongoDBScheduler
    pytq.MongoDBStatusFlagScheduler


if __name__ == "__main__":
    import os

    basename = os.path.basename(__file__)
    pytest.main([basename, "-s", "--tb=native"])
