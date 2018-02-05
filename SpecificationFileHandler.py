from Specification import Specification
from Specification import SpecificationException
from watchdog.events import FileSystemEventHandler

import logging

LOG = logging.getLogger(__name__)


class SpecificationFileHandler(FileSystemEventHandler):
    """
        Takes specification file created events dispatched by the
        observer and processes those events.
    """
    patterns = ["*.csv"]

    def __init__(self, db):
        self.db = db

    def process(self, event):
        full_file_name = event.src_path.split('/')[1]
        file_name = full_file_name[:-4]

        spec = Specification(file_name, self.db)

        if spec.does_specification_exist():
            LOG.error('Specification already exists')
            return

        lines = self.read_file(event.src_path)

        try:
            spec.process_specification(lines)
            LOG.info('Specification successfully added')
        except SpecificationException as error:
            LOG.error(error)

    def on_created(self, event):
        self.process(event)

    def read_file(self, path_to_file):
        """Open specified file and return list of lines stripped of whitespace.
        Args:
            filename (str): name of file
        Returns:
            list: list of lines stripped of whitespace
        """
        with open(path_to_file) as f:
            lines = [line.rstrip('\n') for line in f.readlines()]
        return lines
