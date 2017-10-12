#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
This script implement multi-thread safe, a sqlite backed task queue scheduler.
"""

from pytq import SqliteDictScheduler


# Define your input_data model
class UrlRequest(object):
    def __init__(self, url, context_data=None):
        self.url = url # your have url to crawl
        self.context_data = context_data # and maybe some context data to use


class Scheduler(SqliteDictScheduler):
    # (Required) define how you gonna process your data
    def user_process(self, input_data):
        # you need to implement get_html_from_url yourself
        html = get_html_from_url(input_data.url)

        # you need to implement parse_html yourself
        output_data = parse_html(html)
        return output_data


s = Scheduler(user_db_path=":memory:")

input_data_queue = [
    UrlRequest(url="https://pypi.python.org/pypi/pytq"),
    UrlRequest(url="https://pypi.python.org/pypi/crawlib"),
    UrlRequest(url="https://pypi.python.org/pypi/loggerFactory"),
]

# execute multi thread process
s.do(input_data_queue, multiprocess=True)

# print output
for id, outpupt_data in s.items():
    ...


-------------------------------------------------------------------------------
class Scheduler(SqliteDictScheduler):
    # (Optional) define the identifier of input_data (for duplicate)
    def user_hash_input(self, input_data):
        return input_data.url

    # (Optional) define how do you save output_data to database
    # Here we just use the default one
    def user_post_process(self, task):
        self._default_post_process(task)

    # (Optional) define how do you skip crawled url
    # Here we just use the default one
    def user_is_duplicate(self, task):
        return self._default_is_duplicate(task)

