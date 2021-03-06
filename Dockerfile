# Based on: https://stackoverflow.com/a/58436657/6709902
# parent image
# FROM python:3.7-slim
# FROM armhf/python
FROM armhf/debian

RUN apt-get update \
 && apt-get install apt-utils -y \
# install PostgreSQL ODBC driver
 && apt-get install unixodbc -y \
 && apt-get install unixodbc-dev -y \
#  && apt-get install freetds-dev -y \
#  && apt-get install freetds-bin -y \
#  && apt-get install tdsodbc -y \
 && apt-get install odbc-postgresql -y \
# GCC compilers
 && apt-get install --reinstall build-essential -y \
# SQLAlchemy dependancy
 && apt-get install -y libpq-dev \
# Useful utilities
 && apt-get install screen -y \
 && apt-get install htop -y \
 && apt-get install nano -y \
 && apt-get install procps -y \
 && apt-get install libffi-dev -y \
 && apt-get install zlib1g-dev -y \
# python
 && apt-get -y install python3 \
    python3-pyqt5 \
    python3-pip \
    python3-dev \
    python-software-properties


RUN pip3 install jupyter -U \
 && pip3 install jupyterlab

# populate "ocbcinst.ini"
# RUN echo "[FreeTDS]\n\
# Description = FreeTDS unixODBC Driver\n\
# Driver = /usr/lib/x86_64-linux-gnu/odbc/libtdsodbc.so\n\
# Setup = /usr/lib/x86_64-linux-gnu/odbc/libtdsS.so" >> /etc/odbcinst.ini

COPY requirements.txt ./
RUN pip3 install -r requirements.txt

EXPOSE 5432
EXPOSE 8888

COPY *.py ./
COPY config.json ./

# CMD ["python dataCollector.py --include-test"]
ENTRYPOINT ["tail", "-f", "/dev/null"]
