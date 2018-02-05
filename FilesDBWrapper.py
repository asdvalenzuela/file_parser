from config import get_postgres_config
from psycopg2 import connect as psycopg_connect
from psycopg2 import DatabaseError
from psycopg2 import sql

import logging

LOG = logging.getLogger(__name__)


class FilesDBWrapper(object):
    """ Wrapper for a postgres connection with methods to execute various sql
    queries, inlcuding insert and create, safely.

    The connection is re-used, but the cursors are created and closed per
    method, in keeping with the best practices outlined here:
    http://initd.org/psycopg/docs/faq.html#best-practices

    When a connection exits the with block, if no exception has been raised by
    the block, the transaction is committed.
    In case of exception the transaction is rolled back.

    When a cursor exits the with block it is closed, releasing any resource
    eventually associated with it.
    """

    def __init__(self, test=False):
        if not test:
            self._connection = self._connect()

    def __del__(self):
        self._connection.close()

    def _connect(self):
        try:
            params = get_postgres_config()
            conn = psycopg_connect(**params)
            return conn
        except DatabaseError as error:
            LOG.error(error)

    def query_and_get_id(self, query, params=None):
        id = None
        with self._connection:
            with self._connection.cursor() as curs:
                curs.execute(query, params)
                id = curs.fetchone()[0]
        return id

    def query(self, query, params):
        with self._connection:
            with self._connection.cursor() as curs:
                curs.execute(query, params)

    def query_given_table(self, query_string, table_name):
        with self._connection:
            with self._connection.cursor() as curs:
                curs.execute(sql.SQL(query_string).format(
                    sql.Identifier(table_name)))

    def query_all(self, query, params):
        rows = None
        with self._connection:
            with self._connection.cursor() as curs:
                curs.execute(query, params)
                rows = curs.fetchall()
        return rows

    def insert(self, insert_string, table_name, values):
        with self._connection:
            with self._connection.cursor() as curs:
                curs.execute(sql.SQL(insert_string).format(
                    sql.Identifier(table_name)), values)

    def create(self, create_string, table_name):
        with self._connection:
            with self._connection.cursor() as curs:
                curs.execute(sql.SQL(create_string).format(
                    sql.Identifier(table_name)))
