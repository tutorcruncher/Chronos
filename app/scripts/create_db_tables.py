from app.db import init_db
from app.utils import settings

if __name__ == '__main__':
    assert settings.dev_mode
    init_db()
