# How to run

```
pip install -r requirements.txt
python app.py --production
```

Create a `server.ini` file to configured the server:

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
usage: app.py [-h] [--production | --profile PROFILE_FILENAME]

optional arguments:
  -h, --help            show this help message and exit
  --production          Run in production mode
  --profile PROFILE_FILENAME
                        Run cProfile in developpment mode and record the a .prof file
```

With the `--production` option, the server starts in debug mode.
