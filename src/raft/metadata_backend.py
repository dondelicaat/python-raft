import logging
import pickle


class MetadataBackend:
    def __init__(self, file_handle):
        self.file_handle = file_handle
        try:
            self.file_handle.seek(0)
            state = pickle.load(self.file_handle)
        except EOFError:
            logging.info("File is empty so initialize to default settings and persist.")
            self.state = {'voted_for': None, 'current_term': 0}
            # Write here so we don't have to deal with EOFError in self.read()
            self.write(self.state)
        else:
            self.state = state

    def read(self):
        self.file_handle.seek(0)
        state = pickle.load(self.file_handle)
        logging.debug(f"Read {state} from file")
        return state

    def write(self, state):
        self.file_handle.seek(0)
        pickle.dump(state, self.file_handle)
        logging.debug(f"Wrote {state} to file")
