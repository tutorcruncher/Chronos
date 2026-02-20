"""
Worker startup hook for the job dispatcher.

Automatically starts the continuous dispatcher when a Celery worker
consuming from the 'dispatcher' queue becomes ready.
"""

import logging

from celery.signals import worker_ready

logger = logging.getLogger('chronos.dispatcher')


@worker_ready.connect
def start_dispatcher_on_worker_ready(sender, **kwargs):
    queues = [q.name if hasattr(q, 'name') else q for q in sender.app.amqp.queues.consume_from]
    if 'dispatcher' in queues:
        logger.info('Dispatcher worker ready, starting job dispatcher task')
        # Local import to avoid circular dependency: worker_startup.py is imported
        # at the bottom of worker.py, so worker.py isn't fully loaded yet.
        from chronos.worker import job_dispatcher_task

        job_dispatcher_task.delay()
