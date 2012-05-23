from elasticorm.core.exceptions import ElasticORMException
import pyes
import requests
import simplejson


servers = ["localhost:9200"]

_conn = pyes.ES(servers)
_database = 'test_elasticorm'

def __get_es_connection(database):
    if database is None:
        ElasticORMException("Cannot connect to database 'None'")
    global _database
    _database = database
    global _conn
    return _conn

def get_db():
    global _database
    return _database

class ElasticORMConnection(object):
    
    def __init__(self,database):
        self.es_conn = __get_es_connection(database)
        self.database = get_db() 
    
    def save(self,obj):
        
        self.es_conn.index(obj.__to_elastic_json__(), self.database, obj.type_name, obj.id)
        
def save(obj):
    
    response = _conn.index(obj.__to_elastic_json__(), _database, obj.type_name, obj.id)
    return response
    
def get(type_name,query_args):
    
    global servers
    global _database
    
    query = ''
    for k,v in query_args.items():
        query = "%s%s:%s" % (query,k,v)
    
    type_string=''
    
    if type_name is not None:
        type_string = "/%s" % (type_name)
    
    query_string = ''
        
    if query != '':
        query_string = "q=%s&" % (query)
    
    
    uri = "http://%s/%s%s/_search?%sdefault_operator=AND&sort=created_date:desc" % (servers[0],_database,type_string,query_string)
    print uri
    r = requests.get(uri)
    
    return r

def delete(obj):

    global servers
    global _database

    if obj.id is None:
        return False
    if obj.type_name is None:
        return False
    
    uri = "http://%s/%s/%s/%s" % (servers[0],_database,obj.type_name,obj.id)
    
    r = requests.delete(uri).text
    d = simplejson.loads(r)
    if d.has_key('found'):
        if d['found']:
            if d.has_key('ok'):
                return d['ok']
    return False 
    
def get_by_id(type_name,id):
    
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
    
    return r
