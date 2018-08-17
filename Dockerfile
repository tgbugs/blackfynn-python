FROM python:2.7

ADD requirements.txt /app/requirements.txt
ADD requirements-test.txt /app/requirements-test.txt
RUN pip install -r /app/requirements.txt
RUN pip install -r /app/requirements-test.txt

ADD blackfynn /app/blackfynn
ADD tests     /app/tests
