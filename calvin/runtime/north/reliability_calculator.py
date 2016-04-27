import math
import time

from calvin.utilities.calvinlogger import get_logger

DEFAULT_MTBF = 10000

_log = get_logger(__name__)


class ReliabilityCalculator(object):

    def __init__(self):
        pass

    def calculate_reliability(self, failure_times, replication_time):
        """
        Calculates and returns the probability that a node (which has experinced len(failure_times) failures)
        does not experince any more failure during time replication_time
        """
        _log.debug("Calculating reliability. Failure info {}, replication_time {}".format(
            failure_times, replication_time))

        # Poisson process
        _lambda = self.failure_rate(failure_times, replication_time)
        _log.debug("Lambda: {}".format(_lambda))
        return math.exp(-_lambda)

    def get_mtbf(self, failure_times):
        MTBF = DEFAULT_MTBF  #ms
        _log.debug("Get mtbf from {}".format(failure_times))
        times = sorted(int(t) for t in failure_times)

        if len(times) > 1:
            time_between_failures = [(j - i) for i, j in zip(times, times[1:])]
            MTBF = 1000 * sum(time_between_failures) / len(time_between_failures)

        _log.debug("Calculating mtbf. Failure info {}. mtbf {}".format(
            failure_times, MTBF))

        return MTBF

    def failure_rate(self, failure_times, replication_time):
        # Constant
        MTBF = self.get_mtbf(failure_times)
        fr = float(replication_time) / MTBF
        _log.debug("Failure rate: {}".format(fr))
        return fr

        # Variable failure rate (Curve fitting of failure_times)
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
