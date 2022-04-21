import configparser
import logging
import multiprocessing
import sys

import coloredlogs
import hypercorn.run
from hypercorn.config import Config

production = True if len(sys.argv) > 1 and sys.argv[1] == "prod" else False
coloredlogs.install(
    level="WARNING" if production else "DEBUG",
    fmt="%(asctime)s [%(process)d] %(name)-30.30s %(levelname)-7s %(message)s",
    field_styles=dict(
        asctime=dict(color="blue"),
        process=dict(color="blue"),
        name=dict(color="cyan"),
        hostname=dict(color="magenta"),
        programname=dict(color="cyan"),
        levelname=dict(color="black", bold=True),
        username=dict(color="yellow"),
    ),
)
logging.getLogger("asyncio").setLevel(logging.ERROR)


def read_config(default_config) -> configparser.ConfigParser:
    config = configparser.ConfigParser()
    config.read_string(default_config)
    config.read('server.ini')
    return config


def run(config_mapping, application_path):
    config = Config.from_mapping(config_mapping)
    config.application_path = application_path
    if production:
        if "worker" not in config_mapping:
            config.workers = min(8, multiprocessing.cpu_count())
        if config.accesslog is None:
            config.accesslog = config.errorlog
    else:
        config.accesslog = config.errorlog = logging.getLogger("server")
        config.debug = True
        config.use_reloader = True
        config.access_log_format = '%(h)s "%(r)s" %(s)s %(l)s %(b)s bytes in %(L)s sec'
    try:
        import uvloop

        config.worker_class = "uvloop"
    except ImportError:
        pass

    hypercorn.run.run(config)
