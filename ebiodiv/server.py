import os
import sys
import configparser
import logging
import multiprocessing
import argparse
import cProfile
from pathlib import Path
from os import path
from contextlib import contextmanager

import coloredlogs
import uvicorn
import uvicorn.config
from uvicorn.importer import import_from_string

try:
    import gunicorn
    import gunicorn.app.base
    import uvicorn.workers
    from gunicorn.glogging import Logger
except ImportError:
    # gunicorn doesn't work on Windows
    gunicorn = None


# Debug
LOG_FORMAT_DEBUG = '%(levelname)-7s %(name)-30.30s: %(message)s'

# Production
LOG_FORMAT_PROD = '%(asctime)-15s | %(process)d | %(levelname)s | %(name)s | %(message)s'

CURRENT_DIRECTORY = Path(__file__).parent


## CONFIGURATION

def read_config() -> configparser.ConfigParser:
    config = configparser.ConfigParser()
    config.read(CURRENT_DIRECTORY / "default_config.ini")
    config.read(CURRENT_DIRECTORY.parent / "config.ini")
    return config


def get_worker_count(config):
    if "worker" not in config:
        return min(8, multiprocessing.cpu_count())
    return int(config["worker"])

def parse_args(app_name = ""):
    parser = argparse.ArgumentParser(description=app_name)
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--production', dest='production', action='store_true',
                       help='Run in production mode')
    group.add_argument('--profile', type=str, dest='profile_filename',
                       help='Run cProfile in developpment mode and record the a .prof file',
                       default=None)
    return parser.parse_args()


## GLOBAL VARIABLES

ARGS = parse_args()
CONFIG = read_config()

## LOGGING

def configure_logging():
    global ARGS
    log_level = "INFO" if ARGS.production else "DEBUG"
    log_format = LOG_FORMAT_PROD if ARGS.production else LOG_FORMAT_DEBUG

    logging.getLogger("asyncio").setLevel(logging.ERROR)

    level_styles = {
        'spam': {'color': 'green', 'faint': True},
        'debug': {},
        'notice': {'color': 'magenta'},
        'success': {'bold': True, 'color': 'green'},
        'info': {'bold': True, 'color': 'cyan'},
        'warning': {'color': 'yellow'},
        'error': {'color': 'red'},
        'critical': {'bold': True, 'color': 'red'},
    }
    field_styles = {
        'asctime': {'color': 'green'},
        'process': {'color': 'green'},
        'hostname': {'color': 'magenta'},
        'levelname': {'color': 8},
        'name': {'color': 8},
        'programname': {'color': 'cyan'},
        'username': {'color': 'yellow'},
    }
    if sys.platform == 'win32':
        isatty = False
    else:
        isatty = not ARGS.production
    coloredlogs.install(
        level=log_level,
        level_styles=level_styles,
        field_styles=field_styles,
        fmt=log_format,
        reconfigure=True,
        milliseconds=True,
        isatty=isatty,
    )


def configure_app(app):
    app.on_event("startup")(configure_logging)


## RUN GUNICORN (production)

if gunicorn:
    class UvicornWorker(uvicorn.workers.UvicornWorker):
        CONFIG_KWARGS = {"root_path": CONFIG["server"]["root_path"] }

    class StubbedGunicornLogger(Logger):
        def setup(self, cfg):
            configure_logging()
            self.error_logger = logging.getLogger("gunicorn.error")
            self.access_logger = logging.getLogger("gunicorn.access")
            self.error_logger.setLevel("INFO")
            self.access_logger.setLevel("INFO")
            self.access_logger.addHandler(logging.root.handlers[0])
            self.error_logger.addHandler(logging.root.handlers[0])
            self.error_logger.propagate = False
            self.access_logger.propagate = False


    class StandaloneApplication(gunicorn.app.base.BaseApplication):
        def __init__(self, app_name, options=None):
            self.options = options or {}
            self.application = import_from_string(app_name)
            super().__init__()

        def load_config(self):
            config = {key: value for key, value in self.options.items() if key in self.cfg.settings and value is not None}
            for key, value in config.items():
                self.cfg.set(key.lower(), value)

        def load(self):
            return self.application


    def run_gunicorn(config, app_name, args):
        options = {
            "bind": "%s:%s" % (config["host"], config["port"]),
            "workers": get_worker_count(config),
            "accesslog": "-",
            "errorlog": "-",
            "logger_class": StubbedGunicornLogger,
            "worker_class": "ebiodiv.server.UvicornWorker",
            "default_proc_name": config.get("default_proc_name", "gunicorn"),
            "keyfile": config.get("ssl_keyfile"),
            "certfile": config.get("ssl_certfile"),
        }
        StandaloneApplication(app_name, options).run()


## RUN UVICORN (dev or production on Windows)

@contextmanager
def profile_context(profile_filename):
    logger = logging.getLogger(__name__)
    logger.warning('Start profiling, the reload option is disabled')
    pr = cProfile.Profile()
    pr.enable()
    try:
        yield
    finally:
        pr.disable()
        pr.dump_stats(profile_filename)
        logger.info('Profiling recorded on %s', profile_filename)


def get_reload_excludes():
    if path.exists('.gitignore'):
        with open('.gitignore') as f:
            for file_name in f.readlines():
                yield file_name.replace('\n', '')


def run_uvicorn(config, app_name, args):
    # normal configuration
    uvicorn_logging_config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "default": {
                "()": "uvicorn.logging.DefaultFormatter",
                "fmt": "%(message)s",
                "use_colors": None,
            },
            "access": {
                "()": "uvicorn.logging.AccessFormatter",
                "fmt": '%(client_addr)s - "%(request_line)s" %(status_code)s',  # noqa: E501
            },
        },
        "loggers": {
            "uvicorn": {"level": "INFO", "propagate": True},
            "uvicorn.error": {"level": "ERROR", "propagate": True},
            "uvicorn.access": {"level": "INFO", "propagate": True},
        },
    }

    uvicorn_kwargs = dict(
        host=config["host"],
        port=int(config["port"]),
        root_path=config["root_path"],
        log_level="debug",
        log_config=uvicorn_logging_config,
        access_log=True,
        ssl_keyfile= config.get("ssl_keyfile"),
        ssl_certfile= config.get("ssl_certfile"),
    )

    if args.production:
        uvicorn.run(
            app_name,
            workers = get_worker_count(config),
            **uvicorn_kwargs
        )
    elif not args.profile_filename:
        uvicorn.run(
            app_name,
            reload=True,
            reload_excludes=list(get_reload_excludes()),
            **uvicorn_kwargs
        )
    else:
        with profile_context(args.profile_filename):
            uvicorn.run(
                import_from_string(app_name),
                **uvicorn_kwargs
            )

## RUN

def run(app_name):
    args = parse_args(app_name)
    configure_logging()
    if args.production and gunicorn:
        run_gunicorn(CONFIG["server"], app_name, args)
    else:
        run_uvicorn(CONFIG["server"], app_name, args)
