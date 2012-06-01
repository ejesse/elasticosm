from elasticosm.core.exceptions import ElasticOSMException
from importlib import import_module
import pyes
import requests
import simplejson


servers = ["localhost:9200"]

_conn = pyes.ES(servers)
_database = 'test_elasticosm'

def __get_es_connection(database):
    if database is None:
        ElasticOSMException("Cannot connect to database 'None'")
    global _database
    _database = database
    global _conn
    return _conn

def get_db():
    global _database
    return _database

def get_connection():
    return _conn

class ElasticOSMConnection(object):
    
    def __init__(self,database):
        self.es_conn = __get_es_connection(database)
        self.database = get_db() 
    
    def save(self,obj):
        
        self.es_conn.index(obj.__to_elastic_json__(), self.database, obj.__get_elastic_type_name__(), obj.id)
        
def save(obj):
    
    response = _conn.index(obj.__to_elastic_json__(), _database, obj.__get_elastic_type_name__(), obj.id)
    return response

def fetch(query):
    global servers
    global _database
    
    request_json = query.to_json()
    
    type_string=''
    
    if query.elastic_type is not None:
        from elasticosm.models import ElasticModel
        if query.elastic_type != ElasticModel.__get_elastic_type_name__():
            type_string = "/%s" % (query.elastic_type)
    
    
    uri = "http://%s/%s%s/_search" % (servers[0],_database,type_string)
    print uri
    print request_json
    r = requests.get(uri,data=request_json)
    
    return r

    
def get(type_name,query_args):
    
    global servers
    global _database
    
    from elasticosm.models.queryset import Query
    query = Query.from_query_args(query_args)
    if type_name is not None:
        query.elastic_type = type_name
    
    return fetch(query)

def get_count(type_name=None):
    global _database
    if type_name is not None:
        type_string = "/%s" % (type_name)
    
    uri = "http://%s/%s%s/_count" % (servers[0],_database,type_string)
    r = requests.get(uri)
    json = r.text
    d = simplejson.loads(json)
    return d['count']

def delete(obj):

    global servers
    global _database

    if obj.id is None:
        return False
    if obj.__get_elastic_type_name__() is None:
        return False
    
    uri = "http://%s/%s/%s/%s" % (servers[0],_database,obj.__get_elastic_type_name__(),obj.id)
    
    r = requests.delete(uri).text
    d = simplejson.loads(r)
    if d.has_key('found'):
        if d['found']:
            if d.has_key('ok'):
                return d['ok']
    return False 
    
def get_by_id(id,type_name=None):
    
    global servers
    global _database
    
    if type_name is not None:
        uri = "http://%s/%s/%s/%s" % (servers[0],_database,type_name,id)
        print uri
        r = requests.get(uri)
    else:
        uri = "http://%s/%s/_search?q=id:%s" % (servers[0],_database,id)
        print uri
        r = requests.get(uri)
    d = simplejson.loads(r.text)
    num_hits = d['hits']['total']
    if num_hits < 1:
        return None
    from elasticosm.models.registry import model_registry
    class_type = model_registry[d['hits']['hits'][0]['_type']]
    module = import_module(class_type.__module__)
    _class = getattr(module,class_type.__name__)
    instance = _class()
    obj_dict = d['hits']['hits'][0]['_source']
    for k,v in obj_dict.items():
        instance.__setattr__(k,v)
    return instance
