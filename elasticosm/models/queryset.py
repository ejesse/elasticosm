from elasticosm.core.exceptions import ElasticOSMException
from elasticosm.models import ElasticModel
from urlparse import parse_qs
import requests
import simplejson
import urllib

"""
Super naive basic query object

"""
class Query(object):
    
    def __init__(self):
        self.default_sort = {'created_date' : {'order':'desc'}}
        self.sort = [{'created_date' : {'order':'desc'}}]
        self.term_operand = 'and'
        self.terms = []
        self.elastic_type = None
        self.start_at = None
        self.types = []

    def add_term(self,k,v):
        
        value = v
        from elasticosm.models import ElasticModel
        ## term search for id needs to be vs _id
        if k == 'id':
            k = '_id'
        if isinstance(v,ElasticModel):
            value = v.id
        term = {"term" : {k : value}}
        self.terms.append(term)
        
    def add_sort(self,sort_arg):
        sort_dir = 'desc'
        if sort_arg.find(":") > 0:
            sort_field,sort_dir = sort_arg.split(":")
        else:
            sort_field = sort_arg
        sort_argument = {}
        sort_argument[sort_field] = {'order':sort_dir}
        if len(self.sort) == 1:
            ## override the default if this is the first add call
            if self.sort[0] ==self.default_sort:
                self.sort[0] = sort_argument
            else:
                self.sort.append(sort_argument)

    def to_json(self):
        query_dict = {}
        query_dict['sort'] = self.sort
        
        if len(self.terms) > 0:
            term_query = {self.term_operand : self.terms}
        else:
            term_query = None

        filters = []
        
        if term_query is not None:
            filters.append(term_query)

        if self.elastic_type is not None:
            if self.elastic_type is not ElasticModel.__get_elastic_type_name__():
                from elasticosm.models.registry import ModelRegistry
                registry = ModelRegistry()
                if registry.inheritance_registry.has_key(self.elastic_type):
                    sub_types = registry.inheritance_registry[self.elastic_type]
                    self.types.extend(sub_types)
                    self.types.append(self.elastic_type)
            
        if len(self.types) > 0:
            types = []
            for type_string in self.types:
                type_term = {"term" : {"_type":type_string}}
                types.append(type_term)
            type_filter = {"or":types}
            filters.append(type_filter)
            self.elastic_type = None
            
        if len(filters) == 0:
            filter = None
        elif len(filters) == 1:
            filter = {"filter" : filters[0]}
        else:
            filter = {"filter" : {"and":filters}}
                
        if filter is not None:
            query_dict['query'] = {"constant_score" : filter}
            
        if self.start_at is not None:
            query_dict['from'] = self.start_at
        
        # random???
        if self.sort is not None:
            if len(self.sort) > 0:
                query_dict['sort'] = self.sort
        
        fields = ["_source"]
        
        query_dict['fields'] = fields
        query_dict['size'] = 20
        
        return simplejson.dumps(query_dict)
    
    @staticmethod
    def from_query_args(query_args):
        query = Query()
        for k,v in query_args.items():
            if k == 'start_at':
                try:
                    from_argument=int(v)
                    query.start_at = from_argument
                except:
                    #wtf
                    pass
            elif k == 'sort':
                sort_arg = v
                query.add_sort(sort_arg)
            elif k == 'operand':
                if v == 'or':
                    query.term_operand = 'or'
            else:
                query.add_term(k, v)
    
        return query
    

class QuerySet(object):
    
    def __init__(self,query=None):
        self.items = []
        self.num_items = 0
        self.cursor=0
        self.query=query
        self.__initial_fetch__ = False
        self.limit=None
        
    def __iter__(self):
        return self
    
    def next(self):
        next_cursor = self.cursor + 1
        ok_to_fetch=True
        if self.limit is not None:
            if self.cursor > self.limit:
                raise StopIteration
            elif self.cursor == self.limit:
                ok_to_fetch=False
        if ok_to_fetch:
            if self.__initial_fetch__:
                if self.cursor >= self.num_items:
                    self.cursor = 0
                    raise StopIteration
                if next_cursor >= len(self.items) and next_cursor < self.num_items:
                    self.query.start_at = next_cursor
                    self.__fetch_items__()
            else:
                self.__fetch_items__()

        item = self.items[self.cursor]
        self.cursor = next_cursor
        return item
            
    def count(self):
        if not self.__initial_fetch__:
            self.__fetch_items__()
        return self.num_items

    def sort_by(self,sort):
        if self.query is not None:
            self.query.add_sort(sort)
        return self
        
    def __fetch_items__(self):
        print 'calling fetch'
        print self.query.to_json()
        from elasticosm.core.connection import fetch
        from elasticosm.models import ElasticModel
        json = fetch(self.query)
        
        d = simplejson.loads(json.text)
        if d.has_key('hits'):
            self.num_items = d['hits']['total']
        
            for hit in d['hits']['hits']:
                instance = ElasticModel.from_elastic_dict(hit)
                self.items.append(instance)
        else:
            #FIXME there is probably an error, log
            pass
            
        
        self.__initial_fetch__=True
    
    def __repr__(self):
        if not self.__initial_fetch__ and self.query is not None:
            self.__fetch_items__()
        return self.items.__repr__()
    