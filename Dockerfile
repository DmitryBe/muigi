FROM mesosphere/mesos:0.28.2-2.0.27.ubuntu1404

ENV APP_DIR /app
RUN mkdir -p ${APP_DIR}
WORKDIR ${APP_DIR}

COPY apps/ ${APP_DIR}/apps
COPY client.cfg ${APP_DIR}/
COPY Makefile ${APP_DIR}/
COPY tox.ini ${APP_DIR}/
COPY requirements.txt ${APP_DIR}/

