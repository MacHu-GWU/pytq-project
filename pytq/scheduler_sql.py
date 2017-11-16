#!/usr/bin/env python
# -*- coding: utf-8 -*-

import six
import sqlalchemy as sa
from multiprocessing import cpu_count

try:
    from .scheduler import Task, BaseDBTableBackedScheduler, Encoder
except:  # pragma: no cover
    from pytq.scheduler import Task, BaseDBTableBackedScheduler, Encoder


class SqlScheduler(BaseDBTableBackedScheduler, Encoder):
    """
    :param uri: connection string for sqlaclhemy.create_engine(uri)
    :param table: str or sa.Table. If you use the default post process,
        the table has to have `_out` column. But a string name is recommended.
    """
    uri = None
    table = None

    id_key = "_id"
    """id column name
    """

    out_key = "_out"
    """output column name
    """

    def __init__(self, logger=None, uri=None, table=None):
        super(SqlScheduler, self).__init__(logger=logger)
        self.link_encode_method()
        if uri is None:
            uri = self.uri

        n_cpu = cpu_count()
        if n_cpu >= 8:
            pool_size = n_cpu
        else:
            pool_size = 5
        self.engine = sa.create_engine(uri, pool_size=pool_size)
        self.connection = self.engine.connect()
        self.prepare_table()

    @property
    def uri(self):
        """
        Back-end database connection URI.

        Example: postgresql://username:password@localhost:5432/mydatabase
        """
        raise NotImplementedError

    def prepare_table(self):
        if isinstance(self.table, sa.Table):
            metadata = self.table.metadata
            metadata.create_all(self.engine)
        elif isinstance(self.table, six.string_types):
            metadata = sa.MetaData()
            table = sa.Table(
                self.table, metadata,
                sa.Column(self.id_key, sa.String(), primary_key=True),
                sa.Column(self.out_key, sa.PickleType()),
            )
            metadata.create_all(self.engine)
            self.table = table

    @property
    def table(self):
        """
        sa.Table instance.
        """
        raise NotImplementedError

    @property
    def t(self):
        return self.table

    @property
    def id_col(self):
        return getattr(self.table.c, self.id_key)

    @property
    def out_col(self):
        return getattr(self.table.c, self.out_key)

    def __enter__(self):
        try:
            self.connection.close()
        except:
            pass
        self.connection = self.engine.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.connection.close()

    def connect(self):
        try:
            self.connection.close()
        except:
            pass
        self.connection = self.engine.connect()

    def close(self):
        self.connection.close()

    def _default_is_duplicate(self, task):
        """
        Check if ``task.id`` is presents in primary_key column.
        """
        sql = sa.select(
            [sa.func.count(self.id_col), ]
        ).where(self.id_col == task.id)
        row = self.connection.execute(sql).fetchone()
        if row[0] == 1:
            return True
        else:
            return False

    def _get_finished_id_set(self):
        """
        It's Primary key value set.
        """
        sql = sa.select([self.id_col])
        cursor = self.connection.execute(sql)
        return set([row[0] for row in cursor])

    def _default_post_process(self, task):
        """
        Write serialized output_data to another column.
        """
        ins = self.table.insert()
        row = {
            self.id_key: task.id,
            self.out_key: task.output_data,
        }
        try:
            self.connection.execute(ins, row)
        except sa.exc.IntegrityError:
            row.pop(self.id_key)
            upd = self.table.update(). \
                where(self.id_col == task.id). \
                values(**row)
            self.connection.execute(upd)

    def __len__(self):
        sql = sa.select([sa.func.count(self.id_col), ])
        return self.connection.execute(sql).fetchone()[0]

    def __iter__(self):
        sql = sa.select([self.id_col])
        for row in self.connection.execute(sql):
            yield row[0]

    def clear_all(self):
        self.connection.execute(self.table.delete())

    def get(self, id):
        sql = sa.select([self.out_col]). \
            where(self.id_col == id)
        row = self.connection.execute(sql).fetchone()
        return row[0]