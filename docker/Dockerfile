FROM lemanoman/mysqlshell:python310
WORKDIR /home/python/app
ADD . .
RUN pip3 install -r requirements.txt

ENTRYPOINT ["python3", "main.py"]