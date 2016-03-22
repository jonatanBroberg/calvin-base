import pytest
import unittest
import math
import time

from calvin.runtime.north.reliability_calculator import ReliabilityCalculator


class TestReliabilityCalculator(unittest.TestCase):

	def setUp(self):
		self.reliability_calculator = ReliabilityCalculator()
		self.replication_time = 20

	def testReliabilityNoFailures(self):
		failure_count = 0
		failure_info = []
		node_start_time = time.time()

		reliability = self.reliability_calculator.calculate_reliability(failure_count, failure_info, node_start_time, self.replication_time)
		assert (reliability == math.exp(-float(self.replication_time)/10000))

	def testReliabilityOneFailure(self):
		failure_count = 1
		MTBF = 30
		node_start_time = time.time() - MTBF
		failure_info = [(node_start_time + MTBF, 0.2)]

		reliability = self.reliability_calculator.calculate_reliability(failure_count, failure_info, node_start_time, self.replication_time)
		assert (reliability == math.exp(-float(self.replication_time)/(1000*MTBF)))

	def testReliabilityTwoEqualyDistributedFailures(self):
		failure_count = 2
		MTBF = 30
		node_start_time = time.time() - 2*MTBF
		failure_info = [(node_start_time + MTBF, 0.2), (node_start_time + 2*MTBF, 0.2)]

		reliability = self.reliability_calculator.calculate_reliability(failure_count, failure_info, node_start_time, self.replication_time)
		assert (reliability == math.exp(-float(self.replication_time)/(1000*MTBF)))

	def testReliabilityTwoUnequalDistributedFailures(self):
		failure_count = 2
		MTBF = 30
		node_start_time = time.time() - 2*MTBF
		failure_info = [(node_start_time + 0.3*MTBF, 0.2), (node_start_time + 2*MTBF, 0.2)]

		reliability = self.reliability_calculator.calculate_reliability(failure_count, failure_info, node_start_time, self.replication_time)
		assert (reliability == math.exp(-float(self.replication_time)/(1000*MTBF)))