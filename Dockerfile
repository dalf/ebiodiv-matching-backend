FROM python:3.10-slim

ARG SRV_NAME=ebiodiv
ARG SRV_GID=10977
ARG SRV_UID=10977

EXPOSE 8888

RUN addgroup --gid ${SRV_GID} ${SRV_NAME} && \
    adduser --uid ${SRV_UID} --gid ${SRV_GID} --disabled-password --home /srv --shell /bin/sh ${SRV_NAME}

COPY setup.py requirements.txt README.md .gitignore /srv/
COPY ebiodiv srv/ebiodiv/

RUN chown ${SRV_NAME}:${SRV_NAME} /srv && \
  apt-get update && \
  apt-get install -y build-essential tini && \
  chown -R ${SRV_NAME}:${SRV_NAME} /srv && \
  su ${SRV_NAME} -c "cd /srv; python -m venv venv; . venv/bin/activate; pip install --upgrade pip setuptools wheel; pip install -e ." && \
  apt-get purge -y build-essential && \
  apt-get autoremove -y

USER ${SRV_NAME}
WORKDIR /srv
ENTRYPOINT ["tini", "--"]
CMD ["/srv/venv/bin/ebiodiv-backend", "--production"]
