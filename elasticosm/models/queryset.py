
from elasticosm.models import ElasticModel
from pyes.filters import TermFilter, ORFilter, ANDFilter, ExistsFilter
from pyes.query import FilteredQuery, MatchAllQuery, StringQuery
import json
import logging
logger = logging.getLogger(__name__)


class Query(object):
    """ Super naive basic query object

    """

    def __init__(self):
        self.default_sort = {'created_date': {'order': 'desc'}}
        self.sort = [{'created_date': {'order': 'desc'}}]
        self.term_operand = 'and'
        self.terms = []
        self.search_term = None
        self.elastic_type = None
        self.start_at = None
        self.types = []
        self.size = 20
        self.search_query = MatchAllQuery()

    def add_term(self, k, v):

        values = v
        if not isinstance(v, list):
            values = [v]
        from elasticosm.models import ElasticModel
        ## term search for id needs to be vs _id
        if k.endswith('__in'):
            k = k.rstrip('__in')
            self.term_operand = 'or'
        if k == 'id':
            k = '_id'
        for value in values:
            add_value = value
            if isinstance(value, ElasticModel):
                add_value = value.id
            term = TermFilter(k, add_value)
            self.terms.append(term)

    def add_sort(self, sort_arg):
        sort_dir = 'desc'
        if sort_arg.find(":") > 0:
            sort_field, sort_dir = sort_arg.split(":")
        else:
            sort_field = sort_arg
        sort_argument = {}
        sort_argument[sort_field] = {'order': sort_dir}
        if len(self.sort) == 1:
            ## override the default if this is the first add call
            if self.sort[0] == self.default_sort:
                self.sort[0] = sort_argument
            else:
                self.sort.append(sort_argument)

    def add_exists(self, field_name):

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
                if self.elastic_type in registry.inheritance_registry:
                    sub_types = registry.inheritance_registry[self.elastic_type]
                    self.types.extend(sub_types)
                self.types.append(self.elastic_type)

        if len(self.types) > 0:
            types = []
            for type_string in self.types:
                type_term = TermFilter('_type', type_string)
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
        if len(self.sort) == 1:
            q.sort = self.sort[0]
        else:
            q.sort = self.sort

        return q

    def to_json(self):
        query_dict = {}
        query_dict['sort'] = self.sort

        if len(self.terms) > 0:
            term_query = {self.term_operand: self.terms}
        else:
            term_query = None

        filters = []

        if term_query is not None:
            filters.append(term_query)

        if self.elastic_type is not None:
            if self.elastic_type is not ElasticModel.__get_elastic_type_name__():
                from elasticosm.models.registry import ModelRegistry
                registry = ModelRegistry()
                if self.elastic_type in registry.inheritance_registry:
                    sub_types = registry.inheritance_registry[self.elastic_type]
                    self.types.extend(sub_types)
                    self.types.append(self.elastic_type)

        if len(self.types) > 0:
            types = []
            for type_string in self.types:
                type_term = {"term": {"_type": type_string}}
                types.append(type_term)
            type_filter = {"or": types}
            filters.append(type_filter)
            self.elastic_type = None

        if len(filters) == 0:
            es_filter = None
        elif len(filters) == 1:
            es_filter = {"filter": filters[0]}
        else:
            es_filter = {"filter": {"and": filters}}

        if es_filter is not None:
            es_filter['query'] = {"matchAll": {}}
            query_dict['query'] = {"filtered": es_filter}

        if self.start_at is not None:
            query_dict['from'] = self.start_at

        # random???
        if self.sort is not None:
            if len(self.sort) > 0:
                query_dict['sort'] = self.sort

        fields = ["_source"]

        query_dict['fields'] = fields
        query_dict['size'] = self.size

        return json.dumps(query_dict)

    @staticmethod
    def from_query_args(query_args):
        query = Query()
        for k, v in query_args.items():
            if k == 'start_at':
                try:
                    from_argument = int(v)
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
        return query


class QuerySet(object):

    def __init__(self, query=None):
        self.items = None
        self.cursor = 0
        self.query = query
        self.__initial_fetch__ = False
        self.limit = None

    def _tuple_from_slice(self, i):
        """
        Get (start, end, step) tuple from slice object.
        """
        (start, end, step) = i.indices(len(self.items))
        # Replace (0, -1, 1) with (0, 0, 1) (misfeature in .indices()).
        if step == 1:
            if end < start:
                end = start
                step = None
        if i.step == None:
            step = None
        return (start, end, step)

    def __iter__(self):
        return self
        #self.__initialize_items__()
        #if self.items is None:
        #    items = []
        #else:
        #    items = self.items
        #for i in items:
        #    yield i

    def __return_elastic_model(self, k):
        from elasticosm.models import ElasticModel
        item = self.items.__getitem__(k)
        instance = ElasticModel.from_pyes_model(item)
        return instance

    def __getitem__(self, k):
        self.__initialize_items__()
        if isinstance(k, slice):
            (start, end, step) = self._tuple_from_slice(k)
            if step == None:
                indices = xrange(start, end)
            else:
                indices = xrange(start, end, step)
            return [self.__return_elastic_model(k) for k in indices]
        else:
            return self.__return_elastic_model(k)

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

    def sort_by(self, sort):
        if self.query is None:
            self.query = Query()
        self.query.add_sort(sort)
        return self

    def exists(self, field_name):
        if self.query is None:
            self.query = Query()
        self.query.add_exists(field_name)
        return self

    def get_query_as_json(self):
        return self.query.to_es_query().to_search_json()

    def unpack_query_sort(self, query):
        if isinstance(query.sort, list):
            field = query.sort[0].keys()[0]
            direction = query.sort[0].get(field).get('order')
            return "%s:%s" % (field, direction)

    def __initialize_items__(self):
        if not self.items:
            from elasticosm.core.connection import ElasticOSMConnection
            conn = ElasticOSMConnection()
            logger.debug(self.query.to_es_query().to_search_json())
            self.items = conn.connection.search(
                                        query=self.query.to_es_query(),
                                        indices=conn.get_db(),
                                        sort=self.unpack_query_sort(self.query)
                                        )

