
import pytest

from calvin.runtime.north.resource_manager import ResourceManager

NODE_1_ID = 1
NODE_2_ID = 2


@pytest.fixture
def resource_manager():
    rm = ResourceManager(history_size=3)

    rm.register(NODE_1_ID, 10.0)
    rm.register(NODE_1_ID, 2.0)
    rm.register(NODE_1_ID, 3.0)

    rm.register(NODE_2_ID, 3.0)
    rm.register(NODE_2_ID, 4.0)
    rm.register(NODE_2_ID, 5.0)

    return rm


@pytest.fixture
def empty_resource_manager():
    return ResourceManager()


def test_register_registers_by_node_id(empty_resource_manager):
    empty_resource_manager.register(1, 5.5)
    empty_resource_manager.register(2, 4.5)

    assert len(empty_resource_manager.usages[1]) == 1
    assert empty_resource_manager.usages[1].pop() == 5.5
    assert len(empty_resource_manager.usages[2]) == 1
    assert empty_resource_manager.usages[2].pop() == 4.5


def test_least_busy_returns_least_busy_by_average(resource_manager):
    assert resource_manager.least_busy() == NODE_2_ID


def test_least_busy_returns_most_busy_by_average(resource_manager):
    assert resource_manager.most_busy() == NODE_1_ID


def test_resource_manager_keeps_history_of_max_max_history():
    resource_manager = ResourceManager(2)
    for i in range(10):
        resource_manager.register(1, i)

    assert len(resource_manager.usages[1]) == 2
    assert resource_manager.usages[1].pop() == 9
    assert resource_manager.usages[1].pop() == 8
