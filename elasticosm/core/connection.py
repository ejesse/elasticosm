from elasticosm.core.exceptions import ElasticOSMException
from importlib import import_module
import pyes
import requests
import simplejson


class ElasticOSMConnection(object):
    
    """
    Holds connection information
    Uses the borg pattern, see: http://code.activestate.com/recipes/66531/
    """
    __shared_state = dict(
        databases = [],
        servers=[],
        connection=None,
        default_database=None,
        is_initialized=False,
    )
    
    def __init__(self):
        self.__dict__ = self.__shared_state
        
    def connect(self,settings):
        
        self.default_database = settings.default_database
        self.servers = settings.servers
        self.connection = pyes.ES(self.servers)
        self.is_initialized=True
        from elasticosm.models.registry import ModelRegistry
        register = ModelRegistry()
        apps = settings.ELASTIC_APPS
        for app in apps:
            register.register_models_for_app_name(app)
        #self.es_conn = __get_es_connection(database)
        #self.database = get_db()
        return self

    def _get_connection(self):
        if self.is_initialized:
            return self.connection
        else:
            raise ElasticOSMConnection('Connection not initialized')
    
    def _get_db(self):
        if self.is_initialized:
            return self.default_database 
        else:
            raise ElasticOSMConnection('Connection not initialized')
        
    def _get_server(self):
        if self.is_initialized:
            return self.servers[0] 
        else:
            raise ElasticOSMConnection('Connection not initialized')
    
    @staticmethod
    def get_connection():
        return ElasticOSMConnection()._get_connection()
    
    @staticmethod
    def get_db():
        return ElasticOSMConnection()._get_db()
    
    @staticmethod
    def get_server():
        return ElasticOSMConnection()._get_server()
    
    @staticmethod
    def save(obj):
        
        response = ElasticOSMConnection.get_connection().index(obj.__to_elastic_json__(), ElasticOSMConnection.get_db(), obj.__get_elastic_type_name__(), obj.id)
        return response
    
    @staticmethod
    def fetch(query):
        
        request_json = query.to_json()
        
        type_string=''
        
        if query.elastic_type is not None:
            from elasticosm.models import ElasticModel
            if query.elastic_type != ElasticModel.__get_elastic_type_name__():
                type_string = "/%s" % (query.elastic_type)
        
        
        uri = "http://%s/%s%s/_search" % (ElasticOSMConnection.get_server(),ElasticOSMConnection.get_db(),type_string)
        print uri
        print request_json
        r = requests.get(uri,data=request_json)
        
        return r
    
    @staticmethod        
    def get(type_name,query_args):
        
        from elasticosm.models.queryset import Query
        query = Query.from_query_args(query_args)
        if type_name is not None:
            query.elastic_type = type_name
        
        return ElasticOSMConnection.fetch(query)
    
    @staticmethod
    def get_count(type_name=None):
    
        if type_name is not None:
            from elasticosm.models.registry import ModelRegistry
            registry = ModelRegistry()
            class_type = registry.model_registry[type_name]
            module = import_module(class_type.__module__)
            _class = getattr(module,class_type.__name__)
            instance = _class()
        else:
            from elasticosm.models import ElasticModel
            instance = ElasticModel
            
        return instance.filter().count()
        
    @staticmethod
    def delete(obj):
    
        if obj.id is None:
            return False
        if obj.__get_elastic_type_name__() is None:
            return False
        
        uri = "http://%s/%s/%s/%s" % (ElasticOSMConnection().get_server(),ElasticOSMConnection().get_db(),obj.__get_elastic_type_name__(),obj.id)
        
        r = requests.delete(uri).text
        d = simplejson.loads(r)
        if d.has_key('found'):
            if d['found']:
                if d.has_key('ok'):
                    return d['ok']
        return False 
        
    @staticmethod    
    def get_by_id(id,type_name=None):
        
        if type_name is not None:
            uri = "http://%s/%s/%s/%s" % (ElasticOSMConnection.get_server(),ElasticOSMConnection.get_db(),type_name,id)
            print uri
            r = requests.get(uri)
        else:
            uri = "http://%s/%s/_search?q=id:%s" % (ElasticOSMConnection.get_server(),ElasticOSMConnection.get_db(),id)
            print uri
            r = requests.get(uri)
        d = simplejson.loads(r.text)
        num_hits = d['hits']['total']
        if num_hits < 1:
            return None
        from elasticosm.models.registry import ModelRegistry
        registry = ModelRegistry()
        class_type = registry.model_registry[type_name]
        module = import_module(class_type.__module__)
        _class = getattr(module,class_type.__name__)
        instance = _class()
        obj_dict = d['hits']['hits'][0]['_source']
        for k,v in obj_dict.items():
            instance.__setattr__(k,v)
        return instance

        
        
