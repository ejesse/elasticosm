

class BaseField(object):
    
    def __init__(self,name=None,verbose_name=None,unique=False,required=False,default=None):
        self.name = name
        self.verbose_name = verbose_name
        self.unique = unique
        self.required=required
        self.default=default
        

    def get_value(self, obj):
        return obj.__fields_values__[self.name]
    
    def set_value(self,obj,value):
        obj.__fields_values__[self.name] = value
        
class NumberField(BaseField):
    
    def __init__(self, *args, **kwargs):
        super(NumberField,self).__init__(*args, **kwargs)