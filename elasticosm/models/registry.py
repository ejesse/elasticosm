from elasticosm.core.connection import get_db
import simplejson
import requests
model_registry = {}

__string_field_mapping__ = {"analyzer": "keyword", "type": "string"}

def register_model(model_instance):
    
    global model_registry
    type_name = model_instance.__get_elastic_type_name__()
    if not model_registry.has_key(type_name):
        model_registry[type_name] = model_instance.__class__
        properties = {}
        for field_name, field_instance in model_instance.__fields__.items():
            from elasticosm.models.fields import StringField, ReferenceField
            if isinstance(field_instance,StringField):
                properties[field_name] = __string_field_mapping__
            if isinstance(field_instance,ReferenceField):
                properties[field_name] = __string_field_mapping__
        if len(properties.keys()) > 0:
            mapping_def = {type_name: { 'properties':properties } }
            mapping_json = simplejson.dumps(mapping_def)
            db = get_db()
            uri = "http://localhost:9200/%s/%s/_mapping" % (db,type_name)
            print uri
            r = requests.put(uri,data=mapping_json)
            print r.text