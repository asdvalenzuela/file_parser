from DataFileHandler import DataFileHandler
from FilesDBWrapper import FilesDBWrapper
from SpecificationFileHandler import SpecificationFileHandler
from watchdog.observers import Observer

import time
import logging

LOG = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

if __name__ == '__main__':

    filesDB = FilesDBWrapper()

    spec_observer = Observer()
    spec_observer.schedule(SpecificationFileHandler(filesDB), path='specs')
    spec_observer.start()

    data_observer = Observer()
    data_observer.schedule(DataFileHandler(filesDB), path='data')
    data_observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        spec_observer.stop()
        data_observer.stop()

    spec_observer.join()
    data_observer.join()
