from elasticosm.core.connection import ElasticOSMConnection
from elasticosm.core.exceptions import UniqueFieldExistsException
from elasticosm.models.registry import ModelRegistry
from elasticosm.tests.lorem import LOREM
from elasticosm.tests.models import TestModel, TestModelWithUniqueField
from rsa._version133 import encrypt
from time import sleep
import copy
import datetime
import unittest

""" Config """

class Settings(object):
    pass

settings = Settings() 
settings.ELASTIC_APPS = ()

settings.servers = ["localhost:9500"]

settings.default_database = 'elasticosm_unittests'
settings.TOKEN_ENCRYPTION_KEY = 'XsP57dv/pAMvK5yCVyfAuyhlyYoCarNvQ01aQz9/kzOSYABt9UZFUmQq/aGr0R1kFsXhf7tFxCE='

""" dummy values """

string_field_test_value = 'this is a string field' 
search_field_test_value = LOREM
encrypted_field_test_value = 'this is an encrypted field'
int_field_test_value = 85498430
long_field_test_value = long(4839483)
float_field_test_value = float(4533654)
boolean_field_test_value = True
datetime_field_test_value = datetime.datetime.utcnow()

""" Unit tests """

def assign_test_values_to_fields(model_instance):
    model_instance.string_field = str(string_field_test_value)
    model_instance.search_field = str(search_field_test_value)
    model_instance.encrypted_field = str(encrypted_field_test_value)
    model_instance.int_field = int(int_field_test_value)
    model_instance.long_field = long(long_field_test_value)
    model_instance.float_field = float(float_field_test_value)
    model_instance.boolean_field = True
    model_instance.date_time_field = copy.copy(datetime_field_test_value)

class ElasticosmTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        registry = ModelRegistry()
        t = TestModel()
        conn = ElasticOSMConnection().connect(settings)
        registry.register_model(t)
        
        t = TestModel()

    def test_field_assignment(self):
        
        t = TestModel()
        assign_test_values_to_fields(t)
        self.assertEqual(t.string_field, string_field_test_value)
        self.assertEqual(t.search_field, search_field_test_value)
        self.assertEqual(t.int_field, int_field_test_value)
        self.assertEqual(t.long_field, long_field_test_value)
        self.assertEqual(t.boolean_field, boolean_field_test_value)
        self.assertEqual(t.date_time_field, datetime_field_test_value)
        
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

    def test_encrypt_decrypt(self):

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
        
        self.assertEqual(queried_model.encrypted_field, encrypted_field_test_value)
    
    def test_query(self):
        registry = ModelRegistry()
        t = TestModel()
        conn = ElasticOSMConnection().connect(settings)
        registry.register_model(t)
        assign_test_values_to_fields(t)
        t.save()
        # give it a second to add to the index
        sleep(1)
        queried_models = TestModel.filter(string_field=string_field_test_value)
        models_found = queried_models.count()
        self.assertGreater(models_found, 0, "Only %s TestModels returned from query, should have found at least 1" % (models_found))
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
        sleep(1)
        t.delete()
        sleep(1)
        test_deletes_rs = TestModel.filter(string_field='delete me')
        self.assertEqual(test_deletes_rs.count(), 0)

    def save_model(self,m):
        m.save()

    def test_uniques(self):
        
        registry = ModelRegistry()
        t = TestModelWithUniqueField()
        conn = ElasticOSMConnection().connect(settings)
        registry.register_model(t)
        
        t.string_field_not_unique = 'not unique'
        t.string_field_unique = 'should be unique'
        
        t.save()
        
        t2 = TestModelWithUniqueField()
        
        t2.string_field_not_unique = 'still not unique'
        t2.string_field_unique = 'should be unique'
        
        t3 = TestModelWithUniqueField()
        
        t3.string_field_not_unique = 'not unique'
        t3.string_field_unique = 'should be unique but different this time'
        t3.save()
        
        with self.assertRaises(UniqueFieldExistsException):
            self.save_model(t2)
        
        

    @classmethod        
    def tearDownClass(cls):
        conn = ElasticOSMConnection().connect(settings)
        conn.connection.delete_index_if_exists(settings.default_database)
        
if __name__ == '__main__':
    unittest.main()
    