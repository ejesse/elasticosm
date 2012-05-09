from elasticorm.models.internal_fields import BaseField
from elasticorm.core.connection import get_connection
import re
import simplejson
import uuid

class ModelBase(type):
    """
    Metaclass for all models.
    """
    def __new__(cls, name, bases, attrs):

        super_new = super(ModelBase, cls).__new__
        parents = [b for b in bases if isinstance(b, ModelBase)]
        if not parents:
            # If this isn't a subclass of Model, don't do anything special.
            return super_new(cls, name, bases, attrs)
        
        model_fields = attrs
        
        model_fields['__fields__'] = {}
        model_fields[  'type_name'] = None
        model_fields["version"] = 1
        model_fields["id"] = None
        model_fields["__fields_values__"] = {}
        
        
        
        return super_new(cls, name, bases, model_fields)
        

class BaseElasticModel(object):
    
    __metaclass__ = ModelBase
    
    def __init__(self, *args, **kwargs):
        
        for attr_name in self.__class__.__dict__.keys():
            # skip the built-in stuff, fields should never be called __something
            if not attr_name.startswith('_'):
                attribute_value = self.__class__.__dict__[attr_name]
                print attr_name
                # is it an elasticorm field?
                if isinstance(attribute_value,BaseField):
                    attribute_value.name = attr_name
                    self.__fields__[attr_name] = attribute_value
                    self.__fields_values__[attr_name] = None
                    
        type_name = self.__class__.__dict__.get('type_name',None)
        
        if type_name is None:
            type_name = self.__class__.__name__
        
        self.type_name = self.__clean_type_name__(type_name)
        
        self.version = 0
        self.id = None
        
    def __setattr__(self, name, value, *args, **kwargs):
        if self.__fields__.has_key(name):
            return self.__fields__[name].set_value(self,value)
        else:
            return super(BaseElasticModel,self).__setattr__(name, value)
       
    def __getattribute__(self, name, *args, **kwargs):
        
        ret_val = super(BaseElasticModel,self).__getattribute__(name, *args, **kwargs)
        if isinstance(ret_val,BaseField):
            if self.__fields__.has_key(name):
                return self.__fields__[name].get_value(self)
            else:
                return None
        else:
            return ret_val
            
    def __clean_type_name__(self, type_name):
        s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', type_name)
        clean_name = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()
        clean_name = clean_name.replace(' ','_')
        return clean_name
    
    def __to_elastic_json__(self):
        pass
    
    def save(self):
        if self.id is None:
            self.id = str(uuid.uuid4())