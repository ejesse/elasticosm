from elasticosm.models import StringField, ReferenceField
from elasticosm.models.registry import ModelRegistry
from elasticosm.test_stuff.models import TestModelToo
from elasticosm.core.connection import _conn, _database
from random import choice
import time
import uuid
import requests
words = ['test','stuff','more stuff']

def load_lotso_stuff(num_docs=10000):
    global _conn
    global _database
    
    registry = ModelRegistry()
    
    available_models = registry.model_registry.values()
    
    last_thing = None
    
    bulker_dict = {}
    
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

    start = time.time()
    total_insert_time = 0
    for k,v in bulker_dict.items():
        request = ''
        for item in v:
            request = '%s{ "index" : { "_index" : "%s", "_type" : "%s", "_id" : "%s" } }\n' % (request,_database,item.__get_elastic_type_name__(),item.id)
            request = '%s%s\n' % (request,item.__to_elastic_json__())
        uri = 'http://localhost:9200/_bulk'
        req_start = time.time()
        r = requests.post(uri, request)
        req_end = time.time()
        req_elapsed = req_end - req_start
        print r
        print "took %s to insert %s items" % (req_elapsed,len(v))
        total_insert_time = total_insert_time + req_elapsed
        
    end = time.time()
    
    elapsed = end - start
    creation_time = elapsed - total_insert_time
    
    print "loaded in %s docs in %s seconds on %s seconds to create objects" % (num_docs,total_insert_time, creation_time)