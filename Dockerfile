# Based on: https://stackoverflow.com/a/58436657/6709902
# parent image
FROM python:3.7-slim

# install PostgreSQL ODBC driver
RUN apt-get update \
 && apt-get install unixodbc -y \
 && apt-get install unixodbc-dev -y \
#  && apt-get install freetds-dev -y \
#  && apt-get install freetds-bin -y \
#  && apt-get install tdsodbc -y \
 && apt-get install odbc-postgresql -y \
 && apt-get install --reinstall build-essential -y


# populate "ocbcinst.ini"
# RUN echo "[FreeTDS]\n\
# Description = FreeTDS unixODBC Driver\n\
# Driver = /usr/lib/x86_64-linux-gnu/odbc/libtdsodbc.so\n\
# Setup = /usr/lib/x86_64-linux-gnu/odbc/libtdsS.so" >> /etc/odbcinst.ini


COPY requirements.txt ./
COPY *.py ./
COPY config.json ./

RUN pip install -r requirements.txt

EXPOSE 5432

CMD ["./dataCollector.py"]