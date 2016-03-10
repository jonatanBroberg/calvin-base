import math

class ReliabilityCalculator(object):

	def __init__(self):
		pass

	def calculate_reliability(self, time, MTBF):
		p = math.exp(-time/MTBF)
		return p
