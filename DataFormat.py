import logging
import psycopg2


LOG = logging.getLogger(__name__)


class DataFormatException(Exception):
    pass


class DataFormat(object):
    """
        Represents a data file with a corresponding specification.
        The data in the file will be loaded into the database according to
        its specification.
        Utilizes a db wrapper to execute sql queries and create/edit/get info.

        Attributes:
        spec (object): The specification which corresponds to the data file
        db (object): DB connection wrapper for executing sql queries
        path_to_file (string): Path to where the data file can be found
    """

    def __init__(self, specification, db, path_to_file):
        self.spec = specification
        self.db = db
        self.path_to_file = path_to_file

    def process_data_format(self):
        try:
            if not self.does_data_table_exist():
                self._create_data_format_table()
            self._add_rows_to_data_format_table()
        except DataFormatException as error:
            raise DataFormatException(error)

    def does_data_table_exist(self):
        does_exist = self._get_data_table()
        return does_exist

    def _get_data_table(self):
        query_string = """SELECT * FROM {} LIMIT 1;"""
        try:
            self.db.query_given_table(query_string, self.spec.file_name)
            return True
        except (Exception, psycopg2.DatabaseError):
            return False

    def _create_data_format_table(self):
        self.spec._get_specification_id()
        if self.spec.spec_id:
            self.spec._get_specification_columns()
        if self.spec.columns:

            create_string = self._get_create_sql_string_given_columns(
                self.spec.columns)

            try:
                self.db.create(create_string, self.spec.file_name)
            except (Exception, psycopg2.DatabaseError) as error:
                raise DataFormatException(error)

        else:
            raise DataFormatException('Invalid specification; cannot add ' +
                                      'data format table')

    def _add_rows_to_data_format_table(self):
        self.spec._get_specification_id()
        if self.spec.spec_id:
            self.spec._get_specification_columns()
        if self.spec.columns:

            insert_string = self._get_insert_sql_string_given_columns(
                self.spec.columns)

            # for each line, extract the values to match the columns
            # and insert record into table
            with open(self.path_to_file) as f:
                for line in f.readlines():
                    values = []
                    index = 0
                    for c in range(len(self.spec.columns)):
                        width = self.spec.columns[c][3] + index
                        value = line[index:width]
                        values.append(value.strip())
                        index = width

                        try:
                            self.db.insert(insert_string, self.spec.file_name,
                                           values)
                        except (Exception, psycopg2.DatabaseError) as error:
                            LOG.error(error)

        else:
            raise DataFormatException('Invalid specification; cannot add ' +
                                      'data format rows')

    def _get_create_sql_string_given_columns(self, columns):
        create_string = 'CREATE TABLE {} ('

        columns_string = ''
        for column in columns:
            column_name = column[2].lower()
            column_data_type = column[4]
            columns_string = (columns_string + '\n' + '{} {}' + ',').format(column_name, column_data_type)

        create_string += columns_string[:-1] + ');'

        return create_string

    def _get_insert_sql_string_given_columns(self, columns):
        insert_string = 'INSERT INTO {} VALUES ('

        for c in range(len(columns) - 1):
            insert_string += '%s, '
        insert_string += '%s);'

        return insert_string
