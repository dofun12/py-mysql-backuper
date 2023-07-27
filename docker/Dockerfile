FROM mysql:5.7-debian
RUN mkdir /python
WORKDIR /python



RUN apt-get update -y && apt install -y build-essential zlib1g-dev libncurses5-dev libgdbm-dev libnss3-dev libssl-dev libreadline-dev libffi-dev libsqlite3-dev wget libbz2-dev gzip
RUN wget https://www.python.org/ftp/python/3.10.11/Python-3.10.11.tgz
RUN tar -xf Python-3.10.11.tgz
WORKDIR /python/Python-3.10.11/
RUN ./configure --prefix=/usr/local --enable-optimizations --enable-shared LDFLAGS="-Wl,-rpath /usr/local/lib"
RUN make -j $(nproc)
RUN make altinstall

RUN ln -s /python/Python-3.10.11/python3.10 /usr/bin/python3
RUN ln -s /python/Python-3.10.11/pip3.10 /usr/bin/pip3
RUN ls /python/Python-3.10.11
RUN echo $PATH
RUN pip3.10 install --user --upgrade pip
WORKDIR /app
ADD . .
RUN pip3.10 install -r requirements.txt
#RUN dpkg -i mysql.deb; exit 0
#RUN apt install -f
#RUN apt update
#RUN apt install mysql-apt-config
#RUN apt install mysql-shell -y


ENTRYPOINT [ "python3.10", "main.py"]