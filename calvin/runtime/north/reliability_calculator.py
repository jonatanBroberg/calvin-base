import math
import time

class ReliabilityCalculator(object):

	def __init__(self):
		pass

	def calculate_reliability(self, failure_count, node_start_time, replication_time):
		total_time = time.time() - node_start_time
		MTBF = 1000 * total_time/(failure_count + 1) #milliseconds

		# For a Poisson process with a constant failure rate we get the probability of zero failures to occur in time t as:
		p = math.exp(-float(replication_time)/MTBF)
		return p

		# Derivation to above formula
		"""
		Classic Poisson:
		p = (math.exp(-_lambda) * (_lambda)^n) / (math.factorial(n))
		p = probability that n failures occur when we have _lambda as the event rate, i.e. the average number of failures during time t

		Zero failures gives:
		p = math.exp(-_lambda)

		Average number of failures during time t:
		_lambda = (failure_count + 1)/(total_time) * 1000 * replication_time
		_lambda = (1/MTBF) * replication_time

		Conclusion:
		p = math.exp(-replication_time/MTBF)
		"""

		# Weibull
		"""
		p = math.exp(-(time/delta)^beta)
		Formula taken from "Efficient task replication and management for adaptive fault tolerance in Mobile Grid environments"
		"""