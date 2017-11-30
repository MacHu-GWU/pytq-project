#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pytest
from pytest import raises
from pytq.task import Task, none_or_is_callable


def test_none_or_is_callable():
    with raises(Exception):
        none_or_is_callable("NA", "NA", 1)

    with raises(Exception):
        none_or_is_callable("NA", "NA", "String")

    none_or_is_callable("NA", "NA", None)
    none_or_is_callable("NA", "NA", str)

    def do_nothing(task): return None

    task = Task(
        id=0, input_data=0,
        pre_process=do_nothing, post_process=do_nothing,
    )
    task._pre_process()
    task._post_process()


if __name__ == "__main__":
    import os

    basename = os.path.basename(__file__)
    pytest.main([basename, "-s", "--tb=native"])
