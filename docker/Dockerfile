FROM python:3.10
RUN mkdir /app
WORKDIR /app
ADD . .
RUN pip3 install -r requirements.txt
RUN apt-get update && apt-get install -y --no-install-recommends default-mysql-client
ENTRYPOINT [ "python3", "main.py"]