FROM python:3.6.4-alpine3.7


RUN pip3 install requests


COPY aster_ged_file_list.txt /test/aster_ged_file_list.txt
COPY ceph-read-stress-test.py /test/ceph-read-stress-test.py

WORKDIR /mnt/mesos/sandbox
ENTRYPOINT /test/ceph-read-stress-test.py
