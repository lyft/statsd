FROM lyft/legacy:fc8281097ce3d6a5727c2d38f42a18ec8772a113
COPY ops /code/statsd/ops
COPY requirements.txt /code/statsd/requirements.txt
RUN SERVICE_NAME=statsd CODE_ROOT=/code/statsd /code/containers/legacy/build.sh
COPY . /code/statsd
