from elasticorm.models.internal_fields import BaseField
import re
import simplejson
import uuid

class BaseElasticModel(object):
    
    def __init__(self, *args, **kwargs):
        self.__fields__ = {}
        for attr_name in self.__class__.__dict__.keys():
            # skip the built-in stuff, fields should never be called __something
            if not attr_name.startswith('_'):
                attribute_value = self.__class__.__dict__[attr_name]
                # is it an elasticorm field?
                if isinstance(attribute_value,BaseField):
                    self.__fields__[attr_name] = attribute_value 
        for field in self.__fields__.keys():
            self.__setattr__(field,None)
            
        type_name = self.__class__.__dict__.get('type_name',None)
        
        if type_name is None:
            type_name = self.__class__.__name__
        
        self.type_name = self.__clean_type_name__(type_name)
        
        self.version = 1
        self.id = None
            
    def __clean_type_name__(self, type_name):
        s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', self.__class__.type_name)
        clean_name = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()
        clean_name = clean_name.replace(' ','_')
        return clean_name
    
    def save(self):
        if self.id is None:
            self.id = str(uuid.uuid4())