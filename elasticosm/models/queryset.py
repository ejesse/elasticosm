from elasticosm.core.exceptions import ElasticOSMException
from elasticosm.models import ElasticModel
from pyes.filters import TermFilter, ORFilter, ANDFilter, ExistsFilter
from pyes.query import FilteredQuery, MatchAllQuery, StringQuery
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
        self.search_term = None
        self.elastic_type = None
        self.start_at = None
        self.types = []
        self.size=20
        self.search_query = MatchAllQuery()

    def add_term(self,k,v):
        
        value = v
        from elasticosm.models import ElasticModel
        ## term search for id needs to be vs _id
        if k == 'id':
            k = '_id'
        if isinstance(v,ElasticModel):
            value = v.id
        term = TermFilter(k,value)
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
                
    def add_exists(self,field_name):
        
        self.terms.append(ExistsFilter(field_name))
        return self
    
    def to_es_query(self):
        if self.search_term is not None:
            self.search_query = StringQuery(self.search_term)

        filters = []

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
                type_term = TermFilter('_type',type_string)
                types.append(type_term)
            type_filter = ORFilter(types)
            filters.append(type_filter)
            self.elastic_type = None
            
        if len(self.terms) > 0:
            if self.term_operand == 'or':
                term_query = ORFilter(self.terms)
            else:
                term_query = ANDFilter(self.terms)
            filters.append(term_query)
            
        if len(filters) > 0:
            andq = ANDFilter(filters)
            q = FilteredQuery(self.search_query, andq)
        else:
            q = self.search_query
        
        q.size = self.size
        q.start = self.start_at
        q.sort = self.sort
        
        return q
        

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
            filter['query'] = { "matchAll" : {} }
            query_dict['query'] = {"filtered" : filter}
            
        if self.start_at is not None:
            query_dict['from'] = self.start_at
        
        # random???
        if self.sort is not None:
            if len(self.sort) > 0:
                query_dict['sort'] = self.sort
        
        fields = ["_source"]
        
        query_dict['fields'] = fields
        query_dict['size'] = self.size
        
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
            elif k == 'search_term':
                query.search_term = v
            else:
                query.add_term(k, v)
        print query
        return query
    

class QuerySet(object):
    
    def __init__(self,query=None):
        self.items = None
        self.cursor=0
        self.query=query
        self.__initial_fetch__ = False
        self.limit=None
        
    def __iter__(self):
        return self
    
    def __getitem__(self, k):
        self.__initialize_items__()
        from elasticosm.models import ElasticModel
        item = self.items.__getitem__(k)
        instance = ElasticModel.from_pyes_model(item)
        return instance
    
    def next(self):
        from elasticosm.models import ElasticModel
        self.__initialize_items__()
        try:
            item = self.items.next()
            instance = ElasticModel.from_pyes_model(item)
            return instance
        except Exception, e:
            raise e
            
    def count(self):
        self.__initialize_items__()
        return self.items.count()

    def sort_by(self,sort):
        if self.query is None:
            self.query = Query()
        self.query.add_sort(sort)
        return self
    
    def exists(self,field_name):
        if self.query is None:
            self.query = Query()
        self.query.add_exists(field_name)
        return self
    
    def __initialize_items__(self):
        if not self.items:
            from elasticosm.core.connection import ElasticOSMConnection
            conn = ElasticOSMConnection()
            
            self.items = conn.connection.search(query=self.query.to_es_query(),indices=conn.get_db())            
