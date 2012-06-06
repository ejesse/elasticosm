from elasticosm.core.connection import ElasticOSMConnection
from elasticosm.models import StringField, ReferenceField
from elasticosm.models.registry import ModelRegistry
from elasticosm.test_stuff.models import TestModelToo
from random import choice
import requests
import time
import uuid
words = ['test','stuff','more stuff']

def create_and_save_non_bulk(num_docs=10000):

    registry = ModelRegistry()
    
    available_models = registry.model_registry.values()
    
    start = time.time()
    
    last_thing = None
    
    errors = 0
    
    for i in range(1,num_docs):
        cls = choice(available_models)
        inst = cls()
        if cls is TestModelToo:
            # flip a coin
            eitheror = choice([True,False])
            if eitheror:
                if last_thing is not None:
                    inst.ref=last_thing
        string_fields = []
        for k,v in inst.__fields__.items():
            if v.__class__ is StringField:
                string_fields.append(k)
        word = choice(words)
        setattr(inst, choice(string_fields), word)
        try:
            inst.save()
        except Exception, t:
            print 'TimeError, skipping doc %s' % (t)
            errors = errors + 1
            
            
        last_thing = inst
        
    end = time.time()
    
    elapsed = end - start
    
    print "Created and stored %s docs in %s seconds with %s failures" % (num_docs,elapsed,errors)


def create_objects(num_docs=10000):

    registry = ModelRegistry()
    
    available_models = registry.model_registry.values()
    
    last_thing = None
    
    bulker_dict = {}
    
    start = time.time()
    
    for i in range(1,num_docs):
        cls = choice(available_models)
        inst = cls()
        if cls is TestModelToo:
            # flip a coin
            eitheror = choice([True,False])
            if eitheror:
                if last_thing is not None:
                    inst.ref=last_thing
        string_fields = []
        for k,v in inst.__fields__.items():
            if v.__class__ is StringField:
                string_fields.append(k)
        word = choice(words)
        setattr(inst, choice(string_fields), word)
        
        for v in inst.__fields__.values():
            v.on_save(inst)
        if inst.id is None:
            inst.id = str(uuid.uuid4())
        
        if not bulker_dict.has_key(inst.__get_elastic_type_name__()):
            bulker_dict[inst.__get_elastic_type_name__()] = []
        
        bulker_dict[inst.__get_elastic_type_name__()].append(inst) 
        #inst.save()

    end_creation = time.time()
    
    creation_time = end_creation - start
    
    print '%s documents created in %s seconds, now creating json' % (num_docs,creation_time)

    json_list = []
    
    json_start = time.time()
    
    for k,v in bulker_dict.items():
        request = ''
        for item in v:
            # this is impossible to read but much faster than other concatenation methods
            parts = [request,'{ "index" : { "_index" : "',ElasticOSMConnection.get_db(),'", "_type" : "',item.__get_elastic_type_name__(),'", "_id" : "',item.id,'" } }\n',item.__to_elastic_json__(),'\n']
            request = ''.join(parts)
        json_list.append(request)

    end = time.time()
    
    json_elapsed = end - json_start
    
    print "took %s seconds to create bulk json for %s documents" % (json_elapsed,num_docs)
    
    elapsed = end - start
    
    print "created %s documents in %s seconds" % (num_docs,elapsed)

    return json_list

def insert_bulk_objects(json_list):

    start = time.time()

    for json in json_list:
        uri = 'http://%s/_bulk' % ElasticOSMConnection.get_server()
        req_start = time.time()
        r = requests.post(uri, json)
        req_end = time.time()
        req_elapsed = req_end - req_start
        print "took %s to insert items" % (req_elapsed)
        

    end = time.time()
    
    elapsed = end - start
    
    print "took %s time to insert" % (elapsed)

def load_lotso_stuff(num_docs=10000):

    start = time.time()
    
    bulker_dict = create_objects(num_docs)
    insert_bulk_objects(bulker_dict)
        
    end = time.time()
    
    elapsed = end - start
    
    print "loaded in %s docs in %s seconds on " % (num_docs,elapsed)