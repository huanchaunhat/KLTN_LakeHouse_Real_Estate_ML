FROM python:3.9-slim-bullseye
LABEL maintainer="lakehouse"

ENV DEBIAN_FRONTEND=noninteractive
ENV TERM linux
ENV LANGUAGE=en_US.UTF-8
ENV LANG=en_US.UTF-8
ENV LC_ALL=en_US.UTF-8

# === CÀI ĐẶT CÁC GÓI HỆ THỐNG CẦN THIẾT CHO APP ===
RUN set -ex \
    # build-essential cần cho numpy/scipy
    && buildDeps="git build-essential default-libmysqlclient-dev" \
    && apt-get update -yqq \
    && apt-get upgrade -yqq \
    && apt-get install -yqq --no-install-recommends \
        $buildDeps \
        curl netcat locales openjdk-11-jdk \
    && sed -i 's/^# en_US.UTF-8 UTF-8$/en_US.UTF-8 UTF-8/g' /etc/locale.gen \
    && locale-gen \
    && update-locale LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8 \
    # Tạo user 'app_user' thay vì 'airflow'
    && useradd -ms /bin/bash -d /home/app app_user

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

# COPY app/requirements.txt /requirements.txt
# RUN pip install --no-cache-dir -r /requirements.txt

RUN set -ex \
    && pip install --no-cache-dir -U pip setuptools wheel \
    && pip install --no-cache-dir \
        streamlit==1.15.0 \
        pyspark==3.3.2 \
        delta-spark==2.2.0 \
        findspark \
        pandas==1.4.0 \
        scikit-learn==1.0 \
        numpy \
        scipy \
        requests==2.24.0 \
        altair==4.0 \
        flask \
        mlflow==2.3.2 \
        boto3==1.16.46 \
        awscli \
        pyOpenSSL \
        mysqlclient==2.1.1 \
    && apt-get purge --auto-remove -yqq $buildDeps \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

USER app_user
WORKDIR /home/app

COPY . /home/app

EXPOSE 8051
CMD ["streamlit", "run", "main.py", "--server.port=8051", "--server.address=0.0.0.0"]

# Loại bỏ các lệnh sau vì việc cài đặt requirements đã được thực hiện ở RUN block bên trên
#COPY files/run.sh /
#RUN chmod 777 /home/requirements.txt
#RUN pip install -r /home/requirements.txt
#ENTRYPOINT ["/run.sh"]