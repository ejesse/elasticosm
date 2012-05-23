from elasticorm.core.exceptions import ElasticORMException
import requests, urllib
from urlparse import parse_qs

class QuerySet(object):
    
    def __init__(self,items=[],count=0,cursor=0,query_uri=None,calling_object=None):
        self.items = items
        self.count = count
        self.cursor=cursor
        self.query_uri=query_uri
        self.calling_object=calling_object
        
    def __iter__(self):
        return self
    
    def next(self):
        next_cursor = self.cursor + 1
        if next_cursor > self.count:
            raise StopIteration
        if next_cursor > len(self.items):
            if self.calling_object is None:
                raise ElasticORMException('No reference object to go get further objects. How on earth did this happen?!?!??')
            parsed_uri = requests.utils.urlparse(self.query_uri)
            params = parse_qs(parsed_uri)
            parameters ={}
            for k,v in params.items():
                ## why are they lists?
                parameters[k] = v[0]
            parameters['from'] = next_cursor
            new_uri = "%s%s%s?%s" % (parsed_uri.scheme,parsed_uri.netloc,parsed_uri.path,urllib.urlencode(parameters))
                
            
    def count(self):
        return self.count
    