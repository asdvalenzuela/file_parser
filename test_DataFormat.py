from DataFormat import DataFormat
from DataFormat import DataFormatException
from FilesDBWrapper import FilesDBWrapper
from Specification import Specification

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

        spec_file_name = 'fileformat1'
        path_to_data_file = 'test/fileformat1_2015-06-28.txt'
        self.db = FilesDBWrapper(test=True)
        self.db._connection = psycopg2.connect(**self.postgresql.dsn())
        self.spec = Specification(spec_file_name, self.db)
        self.data_format = DataFormat(self.spec, self.db, path_to_data_file)

    def tearDown(self):
        self.postgresql.stop()

    def test_get_create_sql_string_given_columns(self):
        columns = [(1, 1, 'name', 10, 'TEXT'), (2, 1, 'valid', 1, 'BOOLEAN'),
                   (3, 1, 'count', 3, 'INTEGER')]

        string = self.data_format._get_create_sql_string_given_columns(columns)

        correct_string = 'CREATE TABLE {} (\nname TEXT,\nvalid BOOLEAN,\ncount INTEGER);'
        self.assertEqual(string, correct_string)

    def test_get_insert_sql_string_given_columns(self):
        columns = [(1, 1, 'name', 10, 'TEXT'), (2, 1, 'valid', 1, 'BOOLEAN'),
                   (3, 1, 'count', 3, 'INTEGER')]

        string = self.data_format._get_insert_sql_string_given_columns(columns)

        self.assertEqual(string, 'INSERT INTO {} VALUES (%s, %s, %s);')

    def test_create_data_format_table_no_spec(self):
        with self.assertRaises(DataFormatException):
            self.data_format._create_data_format_table()

    def test_create_data_format_table_spec_exists(self):
        value = self.data_format.does_data_table_exist()
        self.assertFalse(value)

        lines = ['"column name",width,datatype', 'name,10,TEXT',
                 'valid,1,BOOLEAN', 'count,3,INTEGER']
        self.spec.process_specification(lines)
        self.data_format._create_data_format_table()

        value = self.data_format.does_data_table_exist()
        self.assertTrue(value)

    def test_add_rows_to_data_format_table_no_spec(self):
        with self.assertRaises(DataFormatException):
            self.data_format._add_rows_to_data_format_table()

    def test_process_data_format(self):
        value = self.data_format.does_data_table_exist()
        self.assertFalse(value)

        lines = ['"column name",width,datatype', 'name,10,TEXT',
                 'valid,1,BOOLEAN', 'count,3,INTEGER']
        self.spec.process_specification(lines)
        self.data_format.process_data_format()

        value = self.data_format.does_data_table_exist()
        self.assertTrue(value)
