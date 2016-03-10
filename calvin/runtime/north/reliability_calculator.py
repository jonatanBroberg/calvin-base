import math

class ReliabilityCalculator(object):

	def __init__(self):
		pass

	def calculate_reliability(self, time, MTBF):
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
		p = math.exp(-float(time)/MTBF)
		return p
