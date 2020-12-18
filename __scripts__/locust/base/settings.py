import copy
import hashlib
import json
import logging
import os
import threading
import time
from typing import Tuple


# This is the worker exposed http url.
WORKER_URL = os.getenv('WORKER_URL')
if WORKER_URL is None:
    raise RuntimeError('Undefined WORKER_URL')

# This is the watched settings file.
SETTINGS_FILE = 'settings.json'


def get_logger():
    logging.basicConfig()
    li = logging.getLogger()
    li.setLevel(logging.DEBUG)
    li.addHandler(logging.StreamHandler())
    return li


logger = get_logger()


class FileWatcher(threading.Thread):
    _wait_seconds = 3

    def __init__(self, cfg):
        super().__init__()
        self.raw = None
        self.cfg = cfg

    def run(self):
        logger.info('Starting file watcher')
        first_run = True
        while True:
            if not first_run:
                time.sleep(self._wait_seconds)
            first_run = False

            try:
                f = open(SETTINGS_FILE, 'r')
                contents = f.read()
                f.close()
            except FileNotFoundError:
                logger.error('Settings file not found')
                continue

            try:
                loaded = json.loads(contents)
            except json.JSONDecodeError:
                logger.error('Invalid JSON file')
                continue

            if self.raw != contents:
                self.raw = contents
                algo = hashlib.md5()
                algo.update(self.raw.encode('utf-8'))
                self.cfg.update(loaded, algo.hexdigest())


class Manager:
    _LOCK = threading.Lock()
    _INSTANCE = None

    def __init__(self):
        if not self._LOCK.locked():
            raise RuntimeError('use thread-safe get_instance()')
        self._settings = dict()
        self._version = None
        self._lock = threading.Lock()
        self._updater = FileWatcher(self)
        self._updater.start()
        self.state = Manager.State()

    @classmethod
    def get_instance(cls):
        cls._LOCK.acquire()
        if cls._INSTANCE is None:
            cls._INSTANCE = cls()
        cls._LOCK.release()
        return cls._INSTANCE

    def update(self, settings: dict, version: str):
        if not self.validate(settings):
            logger.error('Invalid settings object')
            return
        self._lock.acquire()
        self._settings = settings
        self._version = version
        self._lock.release()
        logger.info('Configuration updated: %s', settings)

    def get(self) -> Tuple[dict, str]:
        self._lock.acquire()
        settings = copy.deepcopy(self._settings)
        version = self._version
        self._lock.release()
        return settings, version

    @staticmethod
    def validate(settings) -> bool:
        if type(settings) is not dict:
            return False
        if type(settings.get('enabled')) not in (bool, type(None)):
            return False
        if type(settings.get('capacity')) not in (int, type(None)):
            return False
        return True

    class State:
        def __init__(self):
            self._container = dict()
            self._lock = threading.Lock()

        def set(self, k, v):
            self._lock.acquire()
            self._container[k] = v
            self._lock.release()

        def get(self, k):
            self._lock.acquire()
            v = self._container.get(k)
            self._lock.release()
            return v
