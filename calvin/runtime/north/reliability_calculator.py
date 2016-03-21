import math
import time

class ReliabilityCalculator(object):

	def __init__(self):
		pass

	def calculate_reliability(self, failure_count, failure_times, node_start_time, replication_time):
		total_time = 1000 * (time.time() - node_start_time)
		if total_time < 1000:
			return 0.8

		# For a Poisson process with a constant failure rate we get the probability of no failures to occur in time t as:
		_lambda = (failure_count + 1)/(total_time) * replication_time
		return math.exp(-_lambda)

		#Just clerifications:
		"""
		Definition Poisson:
		p = (math.exp(-_lambda) * (_lambda)^n) / (math.factorial(n))
		p = probability that n failures occur when we have _lambda as the event rate, i.e. the average number of failures during time t

		Average number of failures during time t:
		_lambda = (failure_count + 1)/(total_time) * 1000 * replication_time
		"""