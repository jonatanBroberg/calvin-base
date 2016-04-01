import math
import time

from calvin.utilities.calvinlogger import get_logger

DEFAULT_MTBF = 10000

_log = get_logger(__name__)


class ReliabilityCalculator(object):

    def __init__(self):
        pass

    def calculate_reliability(self, failure_info, node_start_time, replication_time):
        """
        Calculates and returns the probability that a node (which has experinced len(failure_info) failures)
        does not experince any more failure during time replication_time
        """

        # Poisson process
        _lambda = self.failure_rate(failure_info, node_start_time, replication_time)
        return math.exp(-_lambda)

    def get_mtbf(self, node_start_time, failure_info):
        MTBF = DEFAULT_MTBF  #ms
        times = sorted([info[0] for info in failure_info])

        if len(times) > 1:
            time_between_failures = [(j - i) for i, j in zip(times, times[1:])]
            MTBF = 1000 * sum(time_between_failures) / len(time_between_failures)

        return MTBF

    def failure_rate(self, failure_info, node_start_time, replication_time):
        # Constant
        MTBF = self.get_mtbf(node_start_time, failure_info)
        fr = float(replication_time) / MTBF
        _log.info("Failure rate: {}".format(fr))
        return fr

        # Variable failure rate (Curve fitting of failure_info)
        # ...

        # Variable failure rate (standard bath tub shaped)
        # It is even possible to model since hardware modules are heterogenuous?
        # ...

        # Variable failure rate (Curve fitting of failure_times)
        # ...


        #Just clerifications:
        """
        Definition Poisson:
        p = (math.exp(-_lambda) * (_lambda)^n) / (math.factorial(n))
        p = probability that n failures occur when we have _lambda as the event rate, i.e. the average number of failures during time t

        Average number of failures during time t:
        _lambda = (nbr of failures + 1)/(total_time) * 1000 * replication_time
        """
