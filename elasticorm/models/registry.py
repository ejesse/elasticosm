

model_registry = {}

__string_field_mapping__ = {"analyzer": "keyword", "type": "string"}

def register_model(model_instance):
    
    global __model_registry__
    
    if not __model_registry__.has_key(model_instance.type_name):
        __model_registry__[model_instance.type_name] = model_instance.__class__
        properties = {}
        for field_name, field_instance in model_instance.__fields__.items():
            from elasticorm.models.fields import StringField
            if isinstance(field_instance,StringField):
                properties[field_name] = __string_field_mapping__
            if len(properties.keys()) > 0:
                mapping_def = {model_instance.type_name: { 'properties':properties } }
                # TODO
                # this is test code, don't actually implement to return
                return mapping_def