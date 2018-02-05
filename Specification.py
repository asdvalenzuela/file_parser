import psycopg2
import logging

LOG = logging.getLogger(__name__)


class SpecificationException(Exception):
    pass


class Specification(object):
    """
        Represents a specification file which can be loaded to the database
        with its corresponding columns.
        Utilizes a db wrapper to execute sql queries and create/edit/get info.

        Attributes:
        file_name (object): The name of the file and specification
        db (object): DB connection wrapper for executing sql queries
        spec_id: The id of the specification in the specification_formats table
        columns (list): The list of columns as tuples that specify how a data
            file of that format should be parsed
    """

    def __init__(self, file_name, db):
        self.file_name = file_name
        self.db = db
        self.spec_id = None
        self.columns = None

    def process_specification(self, lines):
        self._insert_specification_format()
        if self.spec_id:
            self.add_specification_columns(lines)
        else:
            raise SpecificationException('Could not add specification')

    def does_specification_exist(self):
        self._get_specification_id()
        if self.spec_id:
            return True
        return False

    def add_specification_columns(self, lines):
        if self.spec_id:
            for index in range(1, len(lines)):
                try:
                    column_name, width, data_type = lines[index].split(',')
                    self._insert_specification_column(column_name,
                                                      width,
                                                      data_type)
                except ValueError:
                    LOG.error('Invalid specification column at line %s' %
                              index)
                    continue

        else:
            LOG.error('Cannot add columns; spec format does not exist.')

    def _get_specification_id(self):
        sql = """SELECT spec_id FROM specification_formats WHERE
                 spec_name = %s;"""
        try:
            self.spec_id = self.db.query_and_get_id(sql, (self.file_name,))
        except (Exception, psycopg2.DatabaseError) as error:
            LOG.error(error)

    def _get_specification_columns(self):
        sql = """SELECT * FROM specification_format_columns WHERE
                 spec_id = %s;"""
        try:
            self.columns = self.db.query_all(sql, (self.spec_id,))
        except (Exception, psycopg2.DatabaseError) as error:
            LOG.error(error)

    def _insert_specification_format(self):
        sql = """INSERT INTO specification_formats(spec_name)
                 VALUES(%s) RETURNING spec_id;"""

        try:
            self.spec_id = self.db.query_and_get_id(sql, (self.file_name,))
        except (Exception, psycopg2.DatabaseError) as error:
            LOG.error(error)

    def _insert_specification_column(self, name, width, data_type):
        sql = """INSERT INTO specification_format_columns(spec_id, column_name,
                 column_width, column_data_type) VALUES(%s, %s, %s, %s);"""

        try:
            self.db.query(sql, (self.spec_id, name, width, data_type))
        except (Exception, psycopg2.DatabaseError) as error:
            LOG.error(error)
