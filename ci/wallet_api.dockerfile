
FROM python:3.9-alpine3.12

LABEL maintainer="Sergey Kovalev <44sergey@gmail.com>"
ENV DOCKERIZE_VERSION v0.6.1

RUN apk add --no-cache --virtual .build-deps gcc libc-dev make openssl \
    && pip install --no-cache-dir "uvicorn[standard]" gunicorn aiobotocore \
    && wget https://github.com/jwilder/dockerize/releases/download/$DOCKERIZE_VERSION/dockerize-alpine-linux-amd64-$DOCKERIZE_VERSION.tar.gz \
    && tar -C /usr/local/bin -xzvf dockerize-alpine-linux-amd64-$DOCKERIZE_VERSION.tar.gz \
    && rm dockerize-alpine-linux-amd64-$DOCKERIZE_VERSION.tar.gz \
    && apk del .build-deps gcc libc-dev make

ARG PIP_PACKAGES_FILE
ENV PIP_PACKAGES_FILE=${PIP_PACKAGES_FILE:-requirements.txt}

COPY requirements/ /home/httpd/app/src/requirements/
RUN pip3 install -U -r /home/httpd/app/src/requirements/${PIP_PACKAGES_FILE}

COPY ./ci/gunicorn.conf.py /home/httpd/conf/gunicorn/
COPY ./ci/maintenance.sh /bin/maintenance.sh
RUN chmod +x /bin/maintenance.sh

COPY src/ /home/httpd/app/src/


WORKDIR /home/httpd/app/src/
ENV PYTHONPATH=/home/httpd/app/src/

EXPOSE 9001
