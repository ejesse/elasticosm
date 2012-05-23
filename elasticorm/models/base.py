from elasticorm.core.connection import save, get, get_by_id, delete as connection_delete
from elasticorm.core.exceptions import MultipleObjectsReturned
from elasticorm.models.internal_fields import BaseField
from importlib import import_module
import datetime
import re
import simplejson
import uuid
import pytz
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
        model_fields["__fields_values__"] = {}
        model_fields['type_name'] = attrs.get('type_name',None)
        model_fields["id"] = None
        
        attrs.update(model_fields)
        
        return super_new(cls, name, bases, attrs)
        

class BaseElasticModel(object):
    
    __metaclass__ = ModelBase
    
    def __init__(self, *args, **kwargs):
        
        # get and set parent class stuff
        for base in self.__class__.__bases__:
            for attr_name,attribute_value in base.__dict__.items():
                if not attr_name.startswith('_'):
                    if isinstance(attribute_value,BaseField):
                        self.__add_elastic_field_to_class__(attr_name,attribute_value)
        for attr_name,attribute_value in self.__class__.__dict__.items():
            # skip the built-in stuff, db/elastic fields should never be called __something or _something
            if not attr_name.startswith('_'):
                #print "%s: %s" % (attr_name, attribute_value)
                # is it an elasticorm field?
                if isinstance(attribute_value,BaseField):
                    self.__add_elastic_field_to_class__(attr_name,attribute_value)

        type_name = self.__class__.__dict__.get('type_name',None)
        
        if type_name is None:
            type_name = self.__class__.__name__
        
        self.type_name = self.__clean_type_name__(type_name)
        
        global __type_names_to_classes__
        __type_names_to_classes__[self.type_name] = self.__class__
        
        self.id = None
        
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
                value = v
                if isinstance(v,datetime.datetime):
                    value = v.isoformat()
                d[k] = value
        d['id'] = self.id
        return simplejson.dumps(d)
    
    def save(self):
        for v in self.__fields__.values():
            v.on_save(self)
        if self.id is None:
            self.id = str(uuid.uuid4())
        r = save(self)
        print r
        
    def get_version(self):
        if self.id is None:
            return 0
        r = get_by_id(self.type_name,self.id)
        d = simplejson.loads(r.text)
        version = d['_version']
        return version


    @classmethod
    def get_type_name(cls):
        try:
            base_type_name = cls.type_name
            if base_type_name is None:
                base_type_name = BaseElasticModel.get_clean_type_name(cls.__name__)
        except AttributeError:
            base_type_name = BaseElasticModel.get_clean_type_name(cls.__name__)
            
        type_name = BaseElasticModel.get_clean_type_name(base_type_name)
        return type_name

    @classmethod
    def get(cls,*args,**kwargs):
        global __type_names_to_classes__

        type_name = cls.get_type_name()
            
        if type_name == 'base_elastic_model':
            type_name = None
        r = get(type_name,kwargs)
        d = simplejson.loads(r.text)
        num_hits = d['hits']['total']
        if num_hits < 1:
            return None
        if num_hits > 1:
            raise MultipleObjectsReturned('Multiple objects returned for query %s' % kwargs)
        obj_dict = d['hits']['hits'][0]['_source']
        class_type = __type_names_to_classes__[d['hits']['hits'][0]['_type']]
        module = import_module(class_type.__module__)
        _class = getattr(module,class_type.__name__)
        instance = _class()
        print obj_dict.items()
        for k,v in obj_dict.items():
            instance.__setattr__(k,v)
        return instance
    
    @classmethod
    def filter(cls,*args,**kwargs):
        global __type_names_to_classes__

        type_name = cls.get_type_name()
        r = get(type_name,kwargs)
        d = simplejson.loads(r.text)
        print d
        num_hits = d['hits']['total']
        items = []
        if num_hits < 1:
            return items
        
        for hit in d['hits']['hits']:
            obj_dict = hit['_source']
            class_type = __type_names_to_classes__[hit['_type']]
            module = import_module(class_type.__module__)
            _class = getattr(module,class_type.__name__)
            instance = _class()
            for k,v in obj_dict.items():
                instance.__setattr__(k,v)
            items.append(instance)
        return items
    
    def delete(self):
        return connection_delete(self)
    