# Based on: https://stackoverflow.com/a/58436657/6709902
# parent image
FROM python:3.7-slim

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
 && apt-get install nano -y
RUN pip install jupyterlab

# populate "ocbcinst.ini"
# RUN echo "[FreeTDS]\n\
# Description = FreeTDS unixODBC Driver\n\
# Driver = /usr/lib/x86_64-linux-gnu/odbc/libtdsodbc.so\n\
# Setup = /usr/lib/x86_64-linux-gnu/odbc/libtdsS.so" >> /etc/odbcinst.ini

COPY requirements.txt ./
RUN pip install -r requirements.txt

EXPOSE 5432
EXPOSE 8888

COPY *.py ./
COPY config.json ./

# CMD ["python dataCollector.py --include-test"]
ENTRYPOINT ["tail", "-f", "/dev/null"]
