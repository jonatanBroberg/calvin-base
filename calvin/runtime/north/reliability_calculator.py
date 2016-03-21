import math
import time

class ReliabilityCalculator(object):

	def __init__(self):
		pass

	def calculate_reliability(self, failure_count, failure_times, node_start_time, replication_time):
		"""
		Calculates and returns the probability that a node (which has experinced failure_count failures)
		does not experince any more failure during time replication_time
		"""

		total_time = 1000 * (time.time() - node_start_time)

		# Poisson process
		_lambda = self.failure_rate(failure_count, failure_times, node_start_time, total_time, replication_time)
		return math.exp(-_lambda)

	def failure_rate(self, failure_count, failure_times, node_start_time, total_time, replication_time):
		# Constant
		MTBF = 10000		#ms
		times = [node_start_time]
		times.extend(failure_times)
		if len(times) > 1:
			time_between_failures = [(j-i) for i,j in zip(times, times[1:])]
			MTBF = 1000 * sum(time_between_failures)/len(time_between_failures)
		return float(replication_time) / MTBF

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
		_lambda = (failure_count + 1)/(total_time) * 1000 * replication_time
		"""