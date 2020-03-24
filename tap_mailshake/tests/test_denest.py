import nose
import unittest
import json
import singer
from tap_ujet.transform import transform_recursive_tree
from tap_ujet.tests.denest_recursive_nodes_data import *

LOGGER = singer.get_logger()

def test_denest_recursive_nodes():
    transformed_data = transform_recursive_tree(MENU_TREE)
    assert not (any('children' in data for data in transformed_data))
    assert len(transformed_data) == 36


if __name__ == '__main__':   
    nose.run()
