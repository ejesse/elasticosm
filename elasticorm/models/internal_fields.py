

class BaseField(object):
    
    def __init__(self,name=None,verbose_name=None,unique=False,required=False,default=None):
        self.name = name
        self.verbose_name = verbose_name
        self.unique = unique
        self.value = None
        self.required=required
        
class NumberField(BaseField):
    
    def __init__(self, *args, **kwargs):
        super(NumberField,self).__init__(*args, **kwargs)