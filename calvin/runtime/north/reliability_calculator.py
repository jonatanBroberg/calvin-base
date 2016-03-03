

class ReliabilityCalculator(object):
	def __init__(self):


	def total_reliability(self, current_nodes):
		if current_nodes is None:
			return 0
		return resource_manager.current_reliability(current_nodes)