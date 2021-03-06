import unittest
import math
import time

from calvin.runtime.north.reliability_calculator import ReliabilityCalculator


class TestReliabilityCalculator(unittest.TestCase):

    def setUp(self):
        self.rc = ReliabilityCalculator()
        self.rc.default_replication_time = 2.0
        self.rc.default_mtbf = 10

    def testReliabilityNoFailures(self):
        failure_info = []

        reliability = self.rc.calculate_reliability(failure_info, self.rc.default_replication_time)
        assert (reliability == math.exp(-float(self.rc.default_replication_time) / self.rc.default_mtbf))

    def testReliabilityOneFailure(self):
        failure_info = [time.time()]

        reliability = self.rc.calculate_reliability(failure_info, self.rc.default_replication_time)
        assert (reliability == math.exp(-float(self.rc.default_replication_time) / self.rc.default_mtbf))

    def testReliabilityTwoFailures(self):
        t = time.time()
        failure_info = [t, t + self.rc.default_mtbf]

        reliability = self.rc.calculate_reliability(failure_info, self.rc.default_replication_time)
        assert (reliability == math.exp(-float(self.rc.default_replication_time) / self.rc.default_mtbf))

    def testReliabilityThreeFailures(self):
        t = time.time()
        failure_info = [t, t + 1.5 * self.rc.default_mtbf, t + 2 * self.rc.default_mtbf]

        reliability = self.rc.calculate_reliability(failure_info, self.rc.default_replication_time)
        assert (reliability == math.exp(-float(self.rc.default_replication_time) / self.rc.default_mtbf))

    def testReplicationTimeConfidence(self):
        rep_times = [10, 10, 20]
        rep_time = self.rc.replication_time(rep_times)
        rep_time
        assert int(rep_time) == 21
        assert self.rc.replication_time(rep_times, 0.99) > rep_time
        assert self.rc.replication_time(rep_times, 1.0) == float('inf')
