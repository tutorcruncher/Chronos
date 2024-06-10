from datetime import datetime, timedelta

import router
from sqlmodel import Session, select

from app.db import engine
from app.sql_models import WebhookLog
from app.utils import app_logger

from fastapi_utilities import repeat_at


@router.on_event('startup')
@repeat_at(cron='0 0 * * *')
async def delete_old_logs():
    """
    We run cron job at midnight every day that wipes all WebhookLogs older than 15 days
    """
    with Session(engine) as db:
        statement = select(WebhookLog).where(
            WebhookLog.timestamp >= datetime.utcnow() - timedelta(days=15)
        )  # need to work out ordering
        results = db.exec(statement)
        app_logger.info(f'Deleting {results.count()} logs')
