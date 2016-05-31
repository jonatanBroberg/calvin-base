from calvin.utilities.calvinlogger import get_logger
from calvin.utilities import calvinconfig
from calvin.runtime.north.reliability_calculator import ReliabilityCalculator

_log = get_logger(__name__)
_conf = calvinconfig.get()

calculators = {
    'ReliabilityCalculator': ReliabilityCalculator
}


def get_reliability_calculator(calc):
    if not calc:
        return ReliabilityCalculator()
    elif calc in calculators:
        return calculators[calc]()
    else:
        raise Exception("Calculator {} not in calculators {}".format(calculators))
