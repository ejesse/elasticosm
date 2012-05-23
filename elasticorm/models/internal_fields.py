

class BaseField(object):
    
    def __init__(self,name=None,verbose_name=None,required=False,default=None):
        self.name = name
        self.verbose_name = verbose_name
        self.required=required
        self.default=default
        

    def get_value(self, obj):
        return obj.__fields_values__[self.name]
    
    def set_value(self,obj,value):
        obj.__fields_values__[self.name] = value
        
    def on_save(self,obj):
        pass
        
class NumberField(BaseField):
    
    def __init__(self, *args, **kwargs):
        super(NumberField,self).__init__(*args, **kwargs)