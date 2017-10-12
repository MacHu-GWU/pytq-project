#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function
from random import randint, sample

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


def validate_schduler_implement(scheduler):
    # Reset a scheduler
    scheduler.clear_all()
    scheduler.log_on()

    scheduler.do(input_data_queue1)
    assert len(scheduler) == len(input_data_queue1)

    scheduler.do(input_data_queue2)
    assert len(scheduler) == len(input_data_queue2)

    for id, output_data in scheduler.items():
        assert int(id) * 1000 == output_data

    scheduler.clear_all()

    scheduler.do(input_data_queue1, multiprocess=True)
    assert len(scheduler) == len(input_data_queue1)

    scheduler.do(input_data_queue2, multiprocess=True)
    assert len(scheduler) == len(input_data_queue2)

    for id, output_data in scheduler.items():
        assert int(id) * 1000 == output_data
