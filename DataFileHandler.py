from DataFormat import DataFormat
from DataFormat import DataFormatException
from Specification import Specification
from watchdog.events import FileSystemEventHandler

import logging

LOG = logging.getLogger(__name__)


class DataFileHandler(FileSystemEventHandler):
    """
        Takes data file 'created' events dispatched by the observer and
        processes those events, finding a corresponding specification format
        in the database, applying that format to the data, and loading the
        data into the database
    """
    patterns = ["*.txt"]

    def __init__(self, db):
        self.db = db

    def process(self, event):
        full_file_name = event.src_path.split('/')[1]
        spec_file_name, date_added_portion = full_file_name.split('_')

        spec = Specification(spec_file_name, self.db)

        if not spec.does_specification_exist():
            LOG.error('Specification does not exist for data file')
            return

        data_format = DataFormat(spec, self.db, event.src_path)

        try:
            data_format.process_data_format()
            LOG.info('Data successfully added')
        except DataFormatException as error:
            LOG.error(error)

    def on_created(self, event):
        self.process(event)
