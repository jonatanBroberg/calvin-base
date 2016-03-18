import math
import time

class ReliabilityCalculator(object):

	def __init__(self):
		pass

	def calculate_reliability(self, failure_count, node_start_time, replication_time):
		total_time = time.time() - node_start_time
		MTBF = total_time/(failure_count + 1)

		# Weibull
		"""
		p = math.exp(-(time/delta)^beta)
		Formula taken from "Efficient task replication and management for adaptive fault tolerance in Mobile Grid environments"
		"""

		#Poisson, failure distibution
		"""
		p = (math.exp(-fail_rate*time) * (fail_rate*time)^n) / (math.factorial(n))
		If n=1 then probability that p is probability that one failure occur in time time  
		"""

		#Exponential
		p = math.exp(-float(replication_time)/(1000*MTBF))
		return p
