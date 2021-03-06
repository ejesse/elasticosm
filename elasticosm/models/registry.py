from elasticosm.core.connection import ElasticOSMConnection
from elasticosm.models.utils import get_all_base_classes, is_elastic_model
import importlib
import inspect
import json
import requests

__string_field_mapping__ = {
                            "analyzer": "keyword",
                            "type": "string"
                            }
__encrypted_field_mapping__ = {
                               "type": "string",
                               "store": "no",
                               "index": "no",
                               "include_in_all": "false"
                               }


class ModelRegistry(object):
    """
    Holds model information
    Uses the borg pattern, see: http://code.activestate.com/recipes/66531/
    """
    __shared_state = dict(
        model_registry={},
        inheritance_registry={},
        instance_registry={},
    )

    def __init__(self):
        self.__dict__ = self.__shared_state

    def register_model(self, model_instance):

        type_name = model_instance.__get_elastic_type_name__()
        if type_name not in self.model_registry:
            self.model_registry[type_name] = model_instance.__class__
            self.instance_registry[type_name] = model_instance.__class__()
            properties = {}
            for field_name, field_instance in model_instance.__fields__.items():
                from elasticosm.models.fields import StringField, ReferenceField
                if isinstance(field_instance, StringField):
                    properties[field_name] = __string_field_mapping__
                if isinstance(field_instance, ReferenceField):
                    properties[field_name] = __string_field_mapping__
                if field_instance.is_encrypted:
                    properties[field_name] = __encrypted_field_mapping__
            if len(properties.keys()) > 0:
                mapping_def = {type_name: {'properties': properties }}
                mapping_json = json.dumps(mapping_def)
                db = ElasticOSMConnection.get_db()
                server = ElasticOSMConnection.get_server()
                http_server = server.replace(":9500", ":9200")
                uri = "http://%s/%s/%s/_mapping" % (http_server, db, type_name)
                r = requests.put(uri, data=mapping_json)
                outcome = json.loads(r.text)
                if 'ok' in outcome:
                    if outcome['ok']:
                        print "Registered model %s on database " + \
                                "%s" % (type_name, db)
                else:
                    print "Failed to register model %s on database " + \
                            "%s with error: %s" % (type_name,
                                                   db,
                                                   outcome.get('error', None)
                                                   )

    def register_models_for_app_name(self, app_name):
        models_module = importlib.import_module("%s.%s" % (app_name, 'models'))
        from elasticosm.models import ElasticModel
        if ElasticModel.__get_elastic_type_name__() in self.inheritance_registry:
            all_inherited_types = self.inheritance_registry[ElasticModel.__get_elastic_type_name__()]
        else:
            all_inherited_types = set()
        for k, v in models_module.__dict__.items():
            if inspect.isclass(v):
                if is_elastic_model(v):
                    self.register_model(v())
                    all_inherited_types.add(v.__get_elastic_type_name__())
                    bases = get_all_base_classes(v)
                    for base in bases:
                        if is_elastic_model(base):
                            if base is not ElasticModel:
                                all_inherited_types.add(
                                                base.__get_elastic_type_name__()
                                                )
                                if base.__get_elastic_type_name__() in self.inheritance_registry:
                                    inherited_types = self.inheritance_registry[base.__get_elastic_type_name__()]
                                else:
                                    inherited_types = set()
                                inherited_types.add(v.__get_elastic_type_name__())
                                self.inheritance_registry[base.__get_elastic_type_name__()] = inherited_types
        self.inheritance_registry[ElasticModel.__get_elastic_type_name__()] = all_inherited_types
