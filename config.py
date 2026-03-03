import os


def _normalize_database_url(raw_url):
    if not raw_url:
        return raw_url
    # Some providers still expose postgres://; SQLAlchemy expects postgresql://.
    if raw_url.startswith("postgres://"):
        return raw_url.replace("postgres://", "postgresql://", 1)
    return raw_url


DATABASE_URL = _normalize_database_url(
    os.getenv("DATABASE_URL")
    or os.getenv("LOCAL_DATABASE_URL")
    or "sqlite:///cx_command_centre.db"
)

SQLALCHEMY_ENGINE_OPTIONS = {
    "pool_pre_ping": True
}
