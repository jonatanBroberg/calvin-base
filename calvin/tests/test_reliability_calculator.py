import pytest
import unittest
import math
import time

from calvin.runtime.north.reliability_calculator import ReliabilityCalculator


class TestReliabilityCalculator(unittest.TestCase):

	def setUp(self):
		self.reliability_calculator = ReliabilityCalculator()
		self.replication_time = 20
		self.mtbf = 10000

	def testReliabilityNoFailures(self):
		failure_info = []

		reliability = self.reliability_calculator.calculate_reliability(failure_info, self.replication_time)
		assert (reliability == math.exp(-float(self.replication_time)/self.mtbf))

	def testReliabilityOneFailure(self):
		failure_info = [time.time()]

		reliability = self.reliability_calculator.calculate_reliability(failure_info, self.replication_time)
		assert (reliability == math.exp(-float(self.replication_time)/self.mtbf))

	def testReliabilityTwoFailures(self):
		t = time.time()
		failure_info = [t, t + self.mtbf]

		reliability = self.reliability_calculator.calculate_reliability(failure_info, self.replication_time)
		assert (reliability == math.exp(-float(self.replication_time)/self.mtbf))

	def testReliabilityThreeFailures(self):
		t = time.time()
		failure_info = [t, t+1.5*self.mtbf, t+2*self.mtbf]

		reliability = self.reliability_calculator.calculate_reliability(failure_info, self.replication_time)
		assert (reliability == math.exp(-float(self.replication_time)/self.mtbf))