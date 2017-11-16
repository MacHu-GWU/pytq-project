#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function
from random import randint, sample
from pytq import Task

n = 20
input_data_queue2 = [i for i in range(1, n + 1)]
input_data_queue1 = list(sample(input_data_queue2, int(n / 4.0)))


class HashAndProcessImplement(object):
    def user_hash_input(self, input_data):
        """
        1 -> "1"
        """
        return str(input_data)

    def user_process(self, input_data):
        """
        1 -> 1000
        """
        return input_data * 1000


def validate_schduler_implement(scheduler,
                                test_multiprocess=True,
                                processes=None):
    """
    :param scheduler: the scheduler you wanna validate with.
    :param test_multiprocess: whether we test with multiprocess.
    :param processes: number of processes we want to use, don't exceed max
        db connection number!
    """
    # Reset a scheduler
    scheduler.clear_all()
    scheduler.log_on()

    scheduler.do(input_data_queue1, ignore_error=False)
    assert len(scheduler) == len(input_data_queue1)

    scheduler.do(input_data_queue2, ignore_error=False)
    assert len(scheduler) == len(input_data_queue2)

    for input_data in input_data_queue2:
        task = Task(id=scheduler._hash_input(
            input_data), input_data=input_data)
        assert scheduler._is_duplicate(task) is True

    for input_data in input_data_queue2:
        scheduler.get_output(input_data)
        assert scheduler.get_output(
            input_data) == scheduler.user_process(input_data)

    for id, output_data in scheduler.items():
        assert int(id) * 1000 == output_data

    if test_multiprocess:
        scheduler.clear_all()

        scheduler.do(
            input_data_queue1,
            multiprocess=True, processes=processes,
            ignore_error=False,
        )
        assert len(scheduler) == len(input_data_queue1)

        scheduler.do(
            input_data_queue2, multiprocess=True, processes=processes,
            ignore_error=False,
        )
        assert len(scheduler) == len(input_data_queue2)

        for id, output_data in scheduler.items():
            assert int(id) * 1000 == output_data
