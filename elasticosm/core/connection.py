from importlib import import_module
import pyes

__uniques_index_settings__ = {'auto_expand_replicas': '0-all',
                              'mappings': {'_default_':
                            {'_all': {'enabled': 0},
                            '_source': {'enabled': 0},
                            '_type': {'index': 'no'},
                            'enabled': 0}},
                              'number_of_shards': 1}


class ElasticOSMConnection(object):

    """
    Holds connection information
    Uses the borg pattern, see: http://code.activestate.com/recipes/66531/
    """
    __shared_state = dict(
        databases=[],
        servers=[],
        encryption_key=None,
        connection=None,
        default_database=None,
        default_database_uniques=None,
        is_initialized=False,
    )

    def __init__(self):
        self.__dict__ = self.__shared_state

    def connect(self, settings):

        self.default_database = settings.default_database
        self.default_database_uniques = "%s__uniques" % self.default_database
        self.servers = settings.servers
        self.connection = pyes.ES(self.servers)
        # create the db/index
        self.connection.create_index_if_missing(self.default_database)
        # create the uniques db/index
        self.connection.create_index_if_missing(self.default_database_uniques,
                                                __uniques_index_settings__)
        self.is_initialized = True
        try:
            self.encryption_key = settings.TOKEN_ENCRYPTION_KEY
        except:
            pass
        from elasticosm.models.registry import ModelRegistry
        register = ModelRegistry()
        apps = settings.ELASTIC_APPS
        for app in apps:
            register.register_models_for_app_name(app)
        #self.es_conn = __get_es_connection(database)
        #self.database = get_db()
        return self

    def get_registered_models(self):
        from elasticosm.models.registry import ModelRegistry
        registry = ModelRegistry()
        all_models = registry.model_registry
        if 'elasticosm-models-ElasticModel' in all_models:
            all_models.pop('elasticosm-models-ElasticModel')
        return all_models

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

    def _get_uniques_db(self):
        if self.is_initialized:
            return self.default_database_uniques
        else:
            raise ElasticOSMConnection('Connection not initialized')

    def _get_server(self):
        if self.is_initialized:
            return self.servers[0]
        else:
            raise ElasticOSMConnection('Connection not initialized')

    @staticmethod
    def search(type_name, search_term, **kwargs):

        query_args = kwargs
        query_args['search_term'] = search_term
        return ElasticOSMConnection.get(type_name, query_args)

    @staticmethod
    def get_connection():
        return ElasticOSMConnection()._get_connection()

    @staticmethod
    def get_db():
        return ElasticOSMConnection()._get_db()

    @staticmethod
    def get_uniques_db():
        return ElasticOSMConnection()._get_uniques_db()

    @staticmethod
    def get_server():
        return ElasticOSMConnection()._get_server()

    @staticmethod
    def save(obj):
        response = ElasticOSMConnection.get_connection().index(
                            obj.__to_elastic_dict__(),
                            ElasticOSMConnection.get_db(),
                            obj.__get_elastic_type_name__(),
                            obj.id)
        return response

    @staticmethod
    def get(type_name, query_args):

        from elasticosm.models.queryset import Query, QuerySet
        query = Query.from_query_args(query_args)
        if type_name is not None:
            query.elastic_type = type_name

        return QuerySet(query=query)

    @staticmethod
    def get_count(type_name=None):

        if type_name is not None:
            from elasticosm.models.registry import ModelRegistry
            registry = ModelRegistry()
            class_type = registry.model_registry[type_name]
            module = import_module(class_type.__module__)
            _class = getattr(module, class_type.__name__)
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
        conn = ElasticOSMConnection()._get_connection()
        result = conn.delete(ElasticOSMConnection().get_db(),
                             obj.__get_elastic_type_name__(),
                             obj.id)
        if 'ok' in result:
            return result['ok']
        return False

    @staticmethod
    def get_by_id(obj_id, type_name=None):
        from elasticosm.models import ElasticModel
        return ElasticModel.filter(id=obj_id)[0]
