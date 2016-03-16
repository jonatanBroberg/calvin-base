import math
import time

class ReliabilityCalculator(object):

	def __init__(self):
		pass

	def calculate_reliability(self, failure_count, node_start_time, replication_time):
		# Weibull
		"""
		p = math.exp(-(time/delta)^beta)
		Formula taken from "Efficient task replication and management for adaptive fault tolerance in Mobile Grid environments"
		"""

		#Poisson
		"""
		p = (math.exp(-fail_rate*time) * (fail_rate*time)^n) / (math.factorial(n))
		If n=1 then probability that p is probability that one failure occur in time time  
		"""

		#Exponential
		if failure_count == 0:
			return 0.8
		total_time = time.time() - node_start_time
		MTBF = total_time/failure_count
		rep_time = 0.1		# Time to replicate an actor
		p = math.exp(-float(replication_time)/(1000*MTBF))
		return p
