#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pytest
from pytq.util import fingerprint


def test_hash_data():
    fingerprint.hash_data({"name": "Alice"})


if __name__ == "__main__":
    import os

    basename = os.path.basename(__file__)
    pytest.main([basename, "-s", "--tb=native"])
