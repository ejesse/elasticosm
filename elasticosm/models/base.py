from elasticosm.core.connection import ElasticOSMConnection
from elasticosm.core.exceptions import MultipleObjectsReturned
from elasticosm.models.internal_fields import BaseField
from elasticosm.models.registry import ModelRegistry
from importlib import import_module
import datetime
import re
import simplejson
import uuid
import pytz
import copy

class ModelBase(type):
    
    """Metaclass for all models."""
    
    def __new__(cls, name, bases, attrs):

        super_new = super(ModelBase, cls).__new__
        parents = [b for b in bases if isinstance(b, ModelBase)]
        if not parents:
            # If this isn't a subclass of Model, don't do anything special.
            return super_new(cls, name, bases, attrs)
        
        model_fields = {}
        model_fields['__fields__'] = {}
        model_fields["__fields_values__"] = {}
        model_fields["__reference_cache__"] = {}
        model_fields["id"] = None
        
        attrs.update(model_fields)
        
        return super_new(cls, name, bases, attrs)
        

class BaseElasticModel(object):
    
    __metaclass__ = ModelBase
    
    def __init__(self, *args, **kwargs):
        
        registry = ModelRegistry()
        origin = registry.instance_registry.get(self.__get_elastic_type_name__(),None)
        if origin is not None:
            # we can use the same field instances for this model
            # across all model instances
            self.__fields__ = origin.__fields__
            # but there might be default values 
            self.__fields_values__ = origin.__fields_values__.copy()
            # and the default values might be reference fields
            self.__reference_cache__ = origin.__reference_cache__.copy()
            self.id = None
            
        else:
        
            if self.__class__ != BaseElasticModel:
            
                self.__fields__ = {}
                self.__fields_values__ = {}
                self.__reference_cache__ = {}
                self.id = None
                
                # get and set parent class stuff
                self.__add_fields_from_class_hierarchy__(self.__class__)
                for attr_name,attribute_value in self.__class__.__dict__.items():
                    # skip the built-in stuff, db/elastic fields should never be called __something or _something
                    if not attr_name.startswith('_'):
                        #print "%s: %s" % (attr_name, attribute_value)
                        # is it an elasticosm field?
                        if isinstance(attribute_value,BaseField):
                            self.__add_elastic_field_to_class__(attr_name,attribute_value)
        
                self.id = None

    def __add_fields_from_class_hierarchy__(self,cls):
        for base in cls.__bases__:
            for attr_name,attribute_value in base.__dict__.items():
                if not attr_name.startswith('_'):
                    if isinstance(attribute_value,BaseField):
                        self.__add_elastic_field_to_class__(attr_name,attribute_value)
            self.__add_fields_from_class_hierarchy__(base)
        
    def __add_elastic_field_to_class__(self,field_name,field_value):
        field_value.name = field_name
        self.__fields__[field_name] = field_value
        self.__fields_values__[field_name] = None
        if field_value.default is not None:
            if callable(field_value.default):
                #python datetime is soooo stooooopid
                if field_value.default == datetime.datetime.utcnow:
                    d = datetime.datetime.utcnow()
                    d = d.replace(tzinfo=pytz.utc)
                    default_value = d
                else:
                    default_value = field_value.default()
            else:
                default_value = field_value.default
            field_value.set_value(self,default_value)
                                
    def __setattr__(self, name, value, *args, **kwargs):
        if self.__fields__.has_key(name):
            return self.__fields__[name].set_value(self,value)
        else:
            return super(BaseElasticModel,self).__setattr__(name, value)
       
    def __getattribute__(self, name, *args, **kwargs):
        # TODO FIXME find a way to get rid of isintance here (slow)
        ret_val = super(BaseElasticModel,self).__getattribute__(name, *args, **kwargs)
        if isinstance(ret_val,BaseField):
            if self.__fields__.has_key(name):
                return self.__fields__[name].get_value(self)
            else:
                return None
        else:
            return ret_val

    @classmethod            
    def __get_elastic_type_name__(cls):
        return "%s.%s" % (cls.__module__,cls.__name__)

    def __to_elastic_dict__(self):
        d = {}
        for k,v in self.__fields_values__.items():
            if v is not None:
                value = v
                if isinstance(v,datetime.datetime):
                    value = v.isoformat()
                d[k] = value
        d['id'] = self.id
        return d
    
    def __to_elastic_json__(self):
        d = self.__to_elastic_dict__()
        return simplejson.dumps(d)
    
    def save(self):
        for v in self.__fields__.values():
            v.on_save(self)
        if self.id is None:
            self.id = str(uuid.uuid4())
        r = ElasticOSMConnection.save(self)
        
    def get_version(self):
        if self.id is None:
            return 0
        r = ElasticOSMConnection.get_by_id(self.type_name,self.id)
        d = simplejson.loads(r.text)
        version = d['_version']
        return version

    @classmethod
    def count(cls):
        return ElasticOSMConnection.get_count(cls.__get_elastic_type_name__())

    @classmethod
    def from_elastic_dict(cls,dict):
        obj_dict = dict['_source']
        from elasticosm.models.registry import ModelRegistry
        registry = ModelRegistry()
        class_type = registry.model_registry[dict['_type']]
        module = import_module(class_type.__module__)
        _class = getattr(module,class_type.__name__)
        instance = _class()
        for k,v in obj_dict.items():
            instance.__setattr__(k,v)
        return instance

    @classmethod
    def from_pyes_model(cls,pyes_model):
        from elasticosm.models.registry import ModelRegistry
        registry = ModelRegistry()
        class_type = registry.model_registry[pyes_model._meta.type]
        module = import_module(class_type.__module__)
        _class = getattr(module,class_type.__name__)
        instance = _class()
        for k,v in pyes_model.items():
            instance.__setattr__(k,v)
        return instance

    @classmethod
    def get(cls,*args,**kwargs):
        type_name = cls.__get_elastic_type_name__()
        from elasticosm.models import ElasticModel
        base_model_type = ElasticModel.__get_elastic_type_name__()
        if type_name == base_model_type:
            type_name = None
        r = ElasticOSMConnection.get(type_name,kwargs)
        if r.count() < 1:
            return None
        if r.count() > 1:
            raise MultipleObjectsReturned('Multiple objects returned for query %s' % kwargs)
        return r[0]
    
    @classmethod
    def filter(cls,*args,**kwargs):
        type_name = cls.__get_elastic_type_name__()
        from elasticosm.models.queryset import Query, QuerySet
        query = Query.from_query_args(kwargs)
        query.elastic_type = type_name
        qs = QuerySet(query=query)
        return qs
    
    @classmethod
    def all(cls):
        return cls.filter({})
    
    def delete(self):
        return ElasticOSMConnection.delete(self)
    