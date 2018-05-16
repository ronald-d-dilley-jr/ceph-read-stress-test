FROM bridge/external:0.7.0.1


COPY aster_ged_file_list.txt /test/aster_ged_file_list.txt
COPY ceph-read-stress-test.py /test/ceph-read-stress-test.py
COPY emissivity_utilities.py /test/emissivity_utilities.py
COPY st_exceptions.py /test/st_exceptions.py
COPY st_utilities.py /test/st_utilities.py


WORKDIR /mnt/mesos/sandbox
ENTRYPOINT /test/ceph-read-stress-test.py
