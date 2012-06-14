from time import sleep
import os
import random
import unittest
import datetime
import copy
from elasticosm.core.connection import ElasticOSMConnection
from elasticosm.models.registry import ModelRegistry
from elasticosm.tests.models import TestModel

"""Configurations"""

class Settings(object):
    pass

settings = Settings() 
settings.ELASTIC_APPS = ()

settings.servers = ["localhost:9200"]

settings.default_database = 'elasticosm_unittests'

""" dummy values """

string_field_test_value = 'this is a string field' 
search_field_test_value = 'this is a search field' 
int_field_test_value = 85498430
long_field_test_value = long(4839483)
float_field_test_value = float(4533654)
boolean_field_test_value = True
datetime_field_test_value = datetime.datetime.utcnow()

"""Unit tests"""

def assign_test_values_to_fields(model_instance):
    model_instance.string_field = str(string_field_test_value)
    model_instance.search_field = str(search_field_test_value)
    model_instance.int_field = int(int_field_test_value)
    model_instance.long_field = long(long_field_test_value)
    model_instance.float_field = float(float_field_test_value)
    model_instance.boolean_field = True
    model_instance.date_time_field = copy.copy(datetime_field_test_value)

class ElasticosmTests(unittest.TestCase):

    def test_field_assignment(self):
        
        t = TestModel()
        assign_test_values_to_fields(t)
        self.assertEqual(t.string_field, string_field_test_value)
        self.assertEqual(t.search_field, search_field_test_value)
        self.assertEqual(t.int_field, int_field_test_value)
        self.assertEqual(t.long_field, long_field_test_value)
        self.assertEqual(t.boolean_field, boolean_field_test_value)
        self.assertEqual(t.date_time_field, datetime_field_test_value)
        
    def test_connection(self):
        
        conn = ElasticOSMConnection().connect(settings)

    def test_index_creation(self):
        
        conn = ElasticOSMConnection().connect(settings)
        conn.connection.create_index_if_missing(conn.default_database)
        
    def test_register_model(self):
        
        registry = ModelRegistry()
        t = TestModel()
        conn = ElasticOSMConnection().connect(settings)
        registry.register_model(t)
        
        t = TestModel()

    def test_save_model(self):
        registry = ModelRegistry()
        t = TestModel()
        conn = ElasticOSMConnection().connect(settings)
        registry.register_model(t)
        
        t = TestModel()
        assign_test_values_to_fields(t)
        t.save()

    def test_get(self):
        registry = ModelRegistry()
        t = TestModel()
        conn = ElasticOSMConnection().connect(settings)
        registry.register_model(t)
        
        t = TestModel()
        assign_test_values_to_fields(t)
        t.save()
        
        # give it a second to add to the index
        sleep(1)
        
        queried_model = TestModel.get(id=t.id)
        
        self.assertEqual(t.id, queried_model.id)

    
    def test_query(self):
        registry = ModelRegistry()
        t = TestModel()
        conn = ElasticOSMConnection().connect(settings)
        registry.register_model(t)
        
        queried_models = TestModel.filter(string_field=string_field_test_value)
        
        for m in queried_models:
            self.assertEqual(m.string_field, string_field_test_value)
        
    def test_deletes(self):
        registry = ModelRegistry()
        t = TestModel()
        conn = ElasticOSMConnection().connect(settings)
        registry.register_model(t)
        
        t = TestModel()
        t.string_field = 'delete me'
        t.save()
        t.delete()
        
        test_deletes_rs = TestModel.filter(string_field='delete me')
        self.assertEqual(test_deletes_rs.count(), 0)
        
    def tearDown(self):
        unittest.TestCase.tearDown(self)
        conn = ElasticOSMConnection().connect(settings)
        conn.connection.delete_index_if_exists(settings.default_database)
        
if __name__ == '__main__':
    unittest.main()
    