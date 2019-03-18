import pytest


"""
Configure a --agent command line argument for py.test that runs agent-
dependent tests.
"""

def pytest_addoption(parser):
    parser.addoption("--agent", action="store_true", help="run agent integration tests")


def pytest_runtest_setup(item):
    if 'agent' in item.keywords and not item.config.getoption("--agent"):
        pytest.skip("Needs --agent option to run")
