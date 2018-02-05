from FilesDBWrapper import FilesDBWrapper
from mock import patch
from Specification import Specification
from Specification import SpecificationException

import psycopg2
import testing.postgresql
import unittest


def handler(postgresql):
    """
        Creates the required tables in the test DB
    """
    conn = psycopg2.connect(**postgresql.dsn())
    cursor = conn.cursor()
    commands = (
        """
        CREATE TABLE specification_formats (
            spec_id SERIAL PRIMARY KEY,
            spec_name VARCHAR(255) NOT NULL
        )
        """,
        """ CREATE TABLE specification_format_columns (
                column_id SERIAL PRIMARY KEY,
                spec_id INTEGER NOT NULL,
                column_name VARCHAR(255) NOT NULL,
                column_width INTEGER NOT NULL,
                column_data_type VARCHAR(255) NOT NULL,
                FOREIGN KEY (spec_id)
                REFERENCES specification_formats (spec_id)
                ON DELETE CASCADE
                )
        """)
    for command in commands:
        cursor.execute(command)
    cursor.close()
    conn.commit()
    conn.close()

# Generates Postgresql class which shares the generated database
Postgresql = testing.postgresql.PostgresqlFactory(cache_initialized_db=True,
                                                  on_initialized=handler)


class SpecificationTest(unittest.TestCase):

    def setUp(self):
        self.postgresql = Postgresql()

        file_name = 'fileformat1'
        self.db = FilesDBWrapper(test=True)
        self.db._connection = psycopg2.connect(**self.postgresql.dsn())
        self.spec = Specification(file_name, self.db)

    def tearDown(self):
        self.postgresql.stop()

    def test_insert_specification(self):
        self.spec._insert_specification_format()

        self.assertEqual(self.spec.spec_id, 1)

    def test_insert_and_get_specification_column(self):
        self.spec._insert_specification_format()

        name = 'valid'
        width = 1
        data_type = 'BOOLEAN'

        self.spec._insert_specification_column(name, width, data_type)
        self.spec._get_specification_columns()

        self.assertEqual(self.spec.columns, [(1, 1, 'valid', 1, 'BOOLEAN')])

    def test_specification_exists(self):
        self.spec._insert_specification_format()

        value = self.spec.does_specification_exist()

        self.assertEqual(value, True)

    def test_specification_does_not_exist(self):
        value = self.spec.does_specification_exist()

        self.assertEqual(value, False)

    def test_add_specification_columns(self):
        self.spec._insert_specification_format()
        lines = ['"column name",width,datatype', 'name,10,TEXT',
                 'valid,1,BOOLEAN', 'count,3,INTEGER']

        self.spec.add_specification_columns(lines)
        self.spec._get_specification_columns()

        self.assertEqual(self.spec.columns[0], (1, 1, 'name', 10, 'TEXT'))
        self.assertEqual(self.spec.columns[1], (2, 1, 'valid', 1, 'BOOLEAN'))
        self.assertEqual(self.spec.columns[2], (3, 1, 'count', 3, 'INTEGER'))

    def test_add_specification_columns_invalid_row(self):
        self.spec._insert_specification_format()
        lines = ['"column name",width,datatype', 'name,10,TEXT',
                 'valid,1BOOLEAN', 'count,3,INTEGER']

        self.spec.add_specification_columns(lines)
        self.spec._get_specification_columns()

        self.assertEqual(self.spec.columns[0], (1, 1, 'name', 10, 'TEXT'))
        self.assertEqual(self.spec.columns[1], (2, 1, 'count', 3, 'INTEGER'))

    def test_process_specification(self):
        lines = ['"column name",width,datatype', 'name,10,TEXT',
                 'valid,1,BOOLEAN', 'count,3,INTEGER']
        self.spec.process_specification(lines)

        self.assertEqual(self.spec.spec_id, 1)

        self.spec._get_specification_columns()

        self.assertEqual(self.spec.columns[0], (1, 1, 'name', 10, 'TEXT'))
        self.assertEqual(self.spec.columns[1], (2, 1, 'valid', 1, 'BOOLEAN'))
        self.assertEqual(self.spec.columns[2], (3, 1, 'count', 3, 'INTEGER'))

    def test_process_specification_cannot_add(self):
        with self.assertRaises(SpecificationException):
            with patch.object(self.spec, '_insert_specification_format'):
                    self.spec.process_specification([])
