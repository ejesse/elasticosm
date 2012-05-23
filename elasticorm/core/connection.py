from elasticorm.core.exceptions import ElasticORMException
import pyes, requests


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
    
def get(type,query_args):
    
    global servers
    global _database
    
    query = ''
    for k,v in query_args.items():
        query = "%s%s:%s" % (query,k,v)
    
    if type is not None:
        uri = "http://%s/%s/%s/_search?q=%s&default_operator=AND" % (servers[0],_database,type,query)
    else:
        uri = "http://%s/%s/_search?q=%s&default_operator=AND" % (servers[0],_database,query)
    print uri
    r = requests.get(uri)
    
    return r
    
def get_by_id(type,id):
    
    global servers
    global _database
    
    if type is not None:
        uri = "http://%s/%s/%s/%s" % (servers[0],_database,type,id)
        print uri
        r = requests.get(uri)
    else:
        uri = "http://%s/%s/_search?q=id:%s" % (servers[0],_database,id)
        print uri
        r = requests.get(uri)
    
    return r
