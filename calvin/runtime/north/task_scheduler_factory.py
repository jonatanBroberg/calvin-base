from calvin.utilities.calvinlogger import get_logger
from calvin.utilities import calvinconfig
from calvin.runtime.north.task_scheduler import ReliabilityScheduler

_log = get_logger(__name__)
_conf = calvinconfig.get()

schedulers = {
    'ReliabilityScheduler': ReliabilityScheduler
}


def get_task_scheduler(rs, resource_manager):
    if not rs:
        ReliabilityScheduler(resource_manager)
        return ReliabilityScheduler(resource_manager)
    elif rs in schedulers:
        return schedulers[rs](resource_manager)
    else:
        raise Exception("Scheduler {} not in schedulers {}".format(schedulers))
