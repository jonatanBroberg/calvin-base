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
		failure_info = []

		reliability = self.reliability_calculator.calculate_reliability(failure_info, self.replication_time)
		assert (reliability == math.exp(-float(self.replication_time)/30000))

	def testReliabilityOneFailure(self):
		MTBF = 30
		failure_info = [(time.time(), 0.2)]

		reliability = self.reliability_calculator.calculate_reliability(failure_info, self.replication_time)
		assert (reliability == math.exp(-float(self.replication_time)/(1000*MTBF)))

	def testReliabilityTwoFailures(self):
		MTBF = 30
		t = time.time()
		failure_info = [(t, 0.2), (t + MTBF, 0.2)]

		reliability = self.reliability_calculator.calculate_reliability(failure_info, self.replication_time)
		assert (reliability == math.exp(-float(self.replication_time)/(1000*MTBF)))

	def testReliabilityThreeFailures(self):
		MTBF = 30
		t = time.time()
		failure_info = [(t, 0.2), (t + 1.5*MTBF, 0.2), (t + 2*MTBF, 0.5)]

		reliability = self.reliability_calculator.calculate_reliability(failure_info, self.replication_time)
		assert (reliability == math.exp(-float(self.replication_time)/(1000*MTBF)))