from elasticosm.core.connection import ElasticOSMConnection
from elasticosm.core.exceptions import UniqueFieldExistsException
from pyes.exceptions import DocumentAlreadyExistsException


class BaseField(object):
    
    def __init__(self,name=None,verbose_name=None,required=False,default=None,encrypt_field=False,unique=False):
        self.name = name
        self.verbose_name = verbose_name
        self.required=required
        self.default=default
        self.is_encrypted=encrypt_field
        self.unique=unique

    def get_value(self, obj):
        return obj.__fields_values__[self.name]
    
    def set_value(self,obj,value):
        obj.__fields_values__[self.name] = value
        
    def on_save(self,obj):
        if obj.__fields_values__[self.name] is None:
            obj.__fields_values__[self.name] = self.default
        if obj.id is None:
            if self.unique:
                unique_field_key = "%s__%s" % (obj.__get_elastic_type_name__(),self.name)
                unique_data = {'_id':obj.__fields_values__[self.name],'_type':unique_field_key}
                try:
                    ElasticOSMConnection.get_connection().index(unique_data,ElasticOSMConnection.get_uniques_db(),unique_data['_id'],unique_data['_type'],op_type='create')
                except DocumentAlreadyExistsException:
                    raise UniqueFieldExistsException('Value %s exists for field %s' % (unique_data['_id'],self.name))
        
class NumberField(BaseField):
    
    def __init__(self, *args, **kwargs):
        super(NumberField,self).__init__(*args, **kwargs)