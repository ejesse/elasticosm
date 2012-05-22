from elasticorm.core.exceptions import MultipleObjectsReturned
from elasticorm.core.connection import save, get, get_by_id
from elasticorm.models.internal_fields import BaseField
from importlib import import_module
import re
import simplejson
import uuid

__type_names_to_classes__ = {}

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
        
        model_fields = {}
        model_fields['__fields__'] = {}
        model_fields['type_name'] = attrs.get('type_name',None)
        model_fields["version"] = 1
        model_fields["id"] = None
        model_fields["__fields_values__"] = {}
        
        attrs.update(model_fields)
        
        return super_new(cls, name, bases, attrs)
        

class BaseElasticModel(object):
    
    __metaclass__ = ModelBase
    
    def __init__(self, *args, **kwargs):
        
        for attr_name in self.__class__.__dict__.keys():
            # skip the built-in stuff, db/elastic fields should never be called __something or _something
            if not attr_name.startswith('_'):
                attribute_value = self.__class__.__dict__[attr_name]
                # is it an elasticorm field?
                if isinstance(attribute_value,BaseField):
                    attribute_value.name = attr_name
                    self.__fields__[attr_name] = attribute_value
                    self.__fields_values__[attr_name] = None
                    if attribute_value.default is not None:
                        attribute_value.set_value(self,attribute_value.default)
                    
        type_name = self.__class__.__dict__.get('type_name',None)
        
        if type_name is None:
            type_name = self.__class__.__name__
        
        self.type_name = self.__clean_type_name__(type_name)
        
        global __type_names_to_classes__
        __type_names_to_classes__[self.type_name] = self.__class__
        
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
        return BaseElasticModel.get_clean_type_name(type_name)
    
    @staticmethod
    def get_clean_type_name(type_name):
        s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', type_name)
        clean_name = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()
        clean_name = clean_name.replace(' ','_')
        return clean_name
    
    def __to_elastic_json__(self):
        d = {}
        for k,v in self.__fields_values__.items():
            if v is not None:
                d[k] = v
        d['id'] = self.id
        return simplejson.dumps(d)
    
    def save(self):
        if self.id is None:
            self.id = str(uuid.uuid4())
        r = save(self)
        print r

    @classmethod
    def get_type_name(cls):
        if cls.type_name:
            return BaseElasticModel.get_clean_type_name(cls.type_name)
        return BaseElasticModel.get_clean_type_name(cls.__name__)

    @classmethod
    def get(cls,*args,**kwargs):
        global __type_names_to_classes__
        if cls.type_name:
            type_name = BaseElasticModel.get_clean_type_name(cls.type_name)
        else:
            type_name = BaseElasticModel.get_clean_type_name(cls.__name__)
        
        r = get(type_name,kwargs)
        d = simplejson.loads(r.text)
        num_hits = d['hits']['total']
        if num_hits < 1:
            return None
        if num_hits > 1:
            raise MultipleObjectsReturned('Multiple objects returned for query %s' % kwargs)
        obj_dict = d['hits']['hits'][0]['_source']
        r = get_by_id(type_name,obj_dict['id'])
        d = simplejson.loads(r.text)
        print d
        obj_dict = d['_source']
        version = d['_version']
        class_type = __type_names_to_classes__[type_name]
        module = import_module(class_type.__module__)
        _class = getattr(module,class_type.__name__)
        instance = _class()
        print obj_dict.items()
        for k,v in obj_dict.items():
            instance.__setattr__(k,v)
        instance.version = version
        return instance
    
    
    