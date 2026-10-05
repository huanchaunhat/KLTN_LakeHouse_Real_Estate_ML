# Sử dụng Spark 3.3.2
FROM docker.io/bitnamilegacy/spark:3.3.2

USER root

# 1. Cài đặt công cụ hệ thống
ARG DEBIAN_SNAPSHOT=20260901T000000Z
RUN printf '%s\n' \
        "deb [check-valid-until=no] https://snapshot.debian.org/archive/debian/${DEBIAN_SNAPSHOT}/ bullseye main" \
        "deb [check-valid-until=no] https://snapshot.debian.org/archive/debian/${DEBIAN_SNAPSHOT}/ bullseye-updates main" \
        "deb [check-valid-until=no] https://snapshot.debian.org/archive/debian-security/${DEBIAN_SNAPSHOT}/ bullseye-security main" \
        > /etc/apt/sources.list \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean \
    && apt-get -o Acquire::Retries=5 -o APT::Update::Error-Mode=any update \
    && apt-get -o Acquire::Retries=5 install -y --no-install-recommends \
        curl \
        wget \
        netcat-openbsd \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# 2. Cài đặt thư viện Python
# Dùng delta-spark==2.2.0 để tương thích với Spark 3.3.2
RUN pip install --no-cache-dir boto3 pandas pyarrow delta-spark==2.2.0 requests==2.32.5

# 3. Tải các file JAR
WORKDIR /opt/bitnami/spark/jars

# Xóa MySQL jar cũ nếu có để tránh xung đột
RUN rm -f mysql-connector*.jar

# --- Tải về ---
# 1. Delta Lake 2.2.0
RUN curl -fL --retry 5 \
    https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.2.0/delta-core_2.12-2.2.0.jar \
    -o /opt/bitnami/spark/jars/delta-core_2.12-2.2.0.jar \
    && curl -fL --retry 5 \
    https://repo1.maven.org/maven2/io/delta/delta-storage/2.2.0/delta-storage-2.2.0.jar \
    -o /opt/bitnami/spark/jars/delta-storage-2.2.0.jar

# 2. Hadoop AWS & AWS SDK
RUN curl -fL --retry 5 \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.2/hadoop-aws-3.3.2.jar \
    -o /opt/bitnami/spark/jars/hadoop-aws-3.3.2.jar \
    && curl -fL --retry 5 \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
    -o /opt/bitnami/spark/jars/aws-java-sdk-bundle-1.12.262.jar

# 3. MySQL Connector
RUN curl -fL --retry 5 \
    https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.28/mysql-connector-java-8.0.28.jar \
    -o /opt/bitnami/spark/jars/mysql-connector-java-8.0.28.jar

# Quay lại thư mục làm việc mặc định và user mặc định
WORKDIR /opt/bitnami/spark
USER 1001
