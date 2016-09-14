FROM mesosphere/mesos:0.28.2-2.0.27.ubuntu1404

RUN apt-get update && apt-get install -y python-pip && apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

WORKDIR /opt/app/

ENV PYTHONPATH /usr/lib/python2.7/site-packages/
COPY . ./

RUN pip install -r requirements.txt
