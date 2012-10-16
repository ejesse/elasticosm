from elasticosm.core.connection import ElasticOSMConnection
from elasticosm.models.utils import get_all_base_classes, is_elastic_model
import importlib
import json
import requests
import inspect

__string_field_mapping__ = {"analyzer": "keyword", "type": "string"}
__encrypted_field_mapping__ = {"type" : "string","store" : "no","index" : "no", "include_in_all" : "false"}


class ModelRegistry(object):
    """
    Holds model information
    Uses the borg pattern, see: http://code.activestate.com/recipes/66531/
    """
    __shared_state = dict(
        model_registry = {},
        inheritance_registry={},
        instance_registry={},
    )
    
    def __init__(self):
        self.__dict__ = self.__shared_state

    def register_model(self,model_instance):
        
        type_name = model_instance.__get_elastic_type_name__()
        if not self.model_registry.has_key(type_name):
            self.model_registry[type_name] = model_instance.__class__
            self.instance_registry[type_name] = model_instance.__class__()
            properties = {}
            for field_name, field_instance in model_instance.__fields__.items():
                from elasticosm.models.fields import StringField, ReferenceField
                if isinstance(field_instance,StringField):
                    properties[field_name] = __string_field_mapping__
                if isinstance(field_instance,ReferenceField):
                    properties[field_name] = __string_field_mapping__
                if field_instance.is_encrypted:
                    properties[field_name] =__encrypted_field_mapping__
            if len(properties.keys()) > 0:
                mapping_def = {type_name: { 'properties':properties } }
                mapping_json = json.dumps(mapping_def)
                db = ElasticOSMConnection.get_db()
                server = ElasticOSMConnection.get_server()
                http_server = server.replace(":9500",":9200")
                uri = "http://%s/%s/%s/_mapping" % (http_server,db,type_name)
                r = requests.put(uri,data=mapping_json)
                outcome = json.loads(r.text)
                if outcome.has_key('ok'):
                    if outcome['ok']:
                        print "Registered model %s on database %s" % (type_name,db)
                else:
                    print "Failed to register model %s on database %s with error: %s" % (type_name,db,outcome.get('error',None))
                
    def register_models_for_app_name(self,app_name):
        models_module = importlib.import_module("%s.%s" % (app_name,'models'))
        from elasticosm.models import ElasticModel
        for k,v in models_module.__dict__.items():
            if inspect.isclass(v):
                if is_elastic_model(v):
                    self.register_model(v())
                    bases = get_all_base_classes(v)
                    for base in bases:
                        if is_elastic_model(base) and (not base is ElasticModel):
                            if self.inheritance_registry.has_key(base.__get_elastic_type_name__()):
                                inherited_types = self.inheritance_registry[base.__get_elastic_type_name__()]
                            else:
                                inherited_types = set()
                            inherited_types.add(v.__get_elastic_type_name__())
                            self.inheritance_registry[base.__get_elastic_type_name__()] = inherited_types