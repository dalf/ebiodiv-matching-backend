# How to run

```
pip install -e .
ebiodiv-backend --production  # or python -m ebiodiv --production
```

Create a `config.ini` file to configured the server:

```ini
[server]
root_path=/
host=localhost
port=8888
default_proc_name=ebiodiv
# worker=8
# ssl_keyfile=
# ssl_certfile=

[datasource]
url=https://tb.plazi.org/GgServer/gbifOccLinkData/
timeout=180
```

# Development

```
usage: python -m ebiodiv [-h] [--production | --profile PROFILE_FILENAME]

optional arguments:
  -h, --help            show this help message and exit
  --production          Run in production mode
  --profile PROFILE_FILENAME
                        Run cProfile in developpment mode and record the a .prof file
```

## debug

Without either option `--production` or option `--profile` option, the server starts in debug mode: enable auto-reload (content referenced by .gitignore is ignored).

## profiling

```
pip install snakeviz
ebiodiv-backend --profile test.prof
snakeviz ./test.prof
```
