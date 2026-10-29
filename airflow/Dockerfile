FROM python:3.9-slim-bullseye
LABEL maintainer="lakehouse"

ENV DEBIAN_FRONTEND=noninteractive
ENV TERM linux

# Airflow Envs
ARG AIRFLOW_VERSION=2.6.1
ARG AIRFLOW_USER_HOME=/usr/local/airflow
ENV AIRFLOW_HOME=${AIRFLOW_USER_HOME}
ENV LANGUAGE en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LC_ALL en_US.UTF-8

# Cài đặt các gói hệ thống
# Đồng bộ snapshot với Spark để tránh URL gói Bullseye bị gỡ khỏi mirror.
# Chỉ bỏ kiểm tra hạn metadata snapshot; vẫn xác minh chữ ký của APT.
ARG DEBIAN_SNAPSHOT=20260901T000000Z
RUN set -ex \
    && printf '%s\n' \
        "deb [check-valid-until=no] https://snapshot.debian.org/archive/debian/${DEBIAN_SNAPSHOT}/ bullseye main" \
        "deb [check-valid-until=no] https://snapshot.debian.org/archive/debian/${DEBIAN_SNAPSHOT}/ bullseye-updates main" \
        "deb [check-valid-until=no] https://snapshot.debian.org/archive/debian-security/${DEBIAN_SNAPSHOT}/ bullseye-security main" \
        > /etc/apt/sources.list \
    && rm -rf /var/lib/apt/lists/* \
    && printf '%s\n' 'Acquire::Retries "5";' 'APT::Update::Error-Mode "any";' \
        > /etc/apt/apt.conf.d/80-build-retries \
    && buildDeps="freetds-dev libkrb5-dev libsasl2-dev libssl-dev libffi-dev libpq-dev git build-essential default-libmysqlclient-dev" \
    && apt-get update -yqq \
    && apt-get upgrade -yqq \
    && apt-get install -yqq --no-install-recommends \
        $buildDeps \
        freetds-bin apt-utils curl rsync netcat locales libsasl2-modules openjdk-11-jdk libmariadb3 \
    && sed -i 's/^# en_US.UTF-8 UTF-8$/en_US.UTF-8 UTF-8/g' /etc/locale.gen \
    && locale-gen \
    && update-locale LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8 \
    && useradd -ms /bin/bash -d ${AIRFLOW_USER_HOME} airflow

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

COPY requirements.txt /requirements.txt
RUN set -ex \
    && pip install --no-cache-dir -U pip setuptools wheel \
    && pip install --no-cache-dir "google-re2==1.1.20240701" \
    && pip install --no-cache-dir -r /requirements.txt \
    && apt-get purge --auto-remove -yqq freetds-dev libkrb5-dev libsasl2-dev libssl-dev libffi-dev libpq-dev git build-essential default-libmysqlclient-dev \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# wheel chỉ dùng lúc build; bản mới xung đột packaging của bộ Airflow/dbt.
RUN pip uninstall -y wheel && pip check

# XGBoost cho price_model. Layer riêng để không phải cài lại toàn bộ Airflow;
# phiên bản phải khớp api/requirements.txt vì API unpickle pipeline đã train.
# xgboost-cpu: bản không kèm NCCL/CUDA (~200 MB).
RUN pip install --no-cache-dir xgboost-cpu==2.1.4 \
        numpy==1.26.4 pandas==2.1.4 scikit-learn==1.6.1 \
    && pip check

COPY script/entrypoint.sh /entrypoint.sh
COPY config/airflow.cfg ${AIRFLOW_USER_HOME}/airflow.cfg

# Tạo sẵn thư mục để volume log mới kế thừa quyền của user airflow.
RUN mkdir -p ${AIRFLOW_USER_HOME}/logs \
    && chown -R airflow: ${AIRFLOW_USER_HOME}
EXPOSE 8088 5555 8793
USER airflow
WORKDIR ${AIRFLOW_USER_HOME}
ENTRYPOINT ["/entrypoint.sh"]
CMD ["webserver"]

USER root
RUN apt-get update && apt-get install -y docker.io && \
    usermod -aG docker airflow && \
    apt-get clean && rm -rf /var/lib/apt/lists/*
USER airflow
