# Superset config, mounted vào container qua SUPERSET_CONFIG_PATH (docker-compose.yml).
# Query đi qua Spark Thrift Server chỉ có 1 core, lần đầu đọc Delta trên MinIO
# có thể mất vài chục giây, nên nâng các ngưỡng timeout mặc định (SQL Lab 30s,
# chart/dashboard 60s). Giữ <= timeout của gunicorn trong docker-compose.yml.
import os

SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "change-this-secret")
# Nền bản đồ cho chart deck.gl (dashboard 1, mục 1.4). Lấy token miễn phí tại
# https://account.mapbox.com rồi đặt MAPBOX_API_KEY trong .env; để trống thì chấm
# vẫn hiện nhưng không có nền bản đồ.
MAPBOX_API_KEY = os.environ.get("MAPBOX_API_KEY", "")
# Không đặt SQLALCHEMY_DATABASE_URI: metadata (dataset, chart, dashboard) vẫn ở
# SQLite mặc định trong volume superset_home như trước khi có file này.

QUERY_TIMEOUT_SEC = 280

SQLLAB_TIMEOUT = QUERY_TIMEOUT_SEC
SUPERSET_WEBSERVER_TIMEOUT = QUERY_TIMEOUT_SEC
SQLLAB_VALIDATION_TIMEOUT = 60

# Cache kết quả chart 1 ngày: dữ liệu Gold chỉ đổi khi pipeline chạy (@daily),
# nên mở lại dashboard không phải query Spark lần nữa.
DATA_CACHE_CONFIG = {
    "CACHE_TYPE": "FileSystemCache",
    "CACHE_DIR": "/app/superset_home/cache",
    "CACHE_DEFAULT_TIMEOUT": 24 * 60 * 60,
    "CACHE_KEY_PREFIX": "superset_data_",
}
