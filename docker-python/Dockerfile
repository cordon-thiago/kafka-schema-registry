FROM python:3.6

COPY docker-python/requirements.txt /tmp/pip-tmp/
RUN pip --disable-pip-version-check --no-cache-dir install -r /tmp/pip-tmp/requirements.txt \
    && rm -rf /tmp/pip-tmp

ADD python-app/ /python-app

CMD ["python3"]