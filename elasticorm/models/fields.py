from elasticorm.models.internal_fields import BaseField, NumberField
import datetime


class StringField(BaseField):
    
    def __init__(self, *args, **kwargs):
        super(StringField,self).__init__(*args, **kwargs)
        
    def set_value(self,obj,value):
        if value is None:
            obj.__fields_values__[self.name] = None
        else:
            if isinstance(value,str):
                value = unicode(value)
            if not isinstance(value,unicode):
                raise TypeError('Cant set StringField to non-string (str) type')
            obj.__fields_values__[self.name] = value
        
class TextField(StringField):

    def __init__(self, *args, **kwargs):
        super(TextField,self).__init__(*args, **kwargs)
        
    def set_value(self,obj,value):
        if value is None:
            obj.__fields_values__[self.name] = None
        else:
            if isinstance(value,str):
                value = unicode(value)
            if not isinstance(value,unicode):
                raise TypeError('Cant set TextField to non-string (str) type')
            obj.__fields_values__[self.name] = value
        
class IntField(NumberField):
    
    def __init__(self, *args, **kwargs):
        super(IntField,self).__init__(*args, **kwargs)

    def set_value(self,obj,value):
        if value is None:
            obj.__fields_values__[self.name] = None
        else:
            if not isinstance(value,int):
                raise TypeError('Cant set IntField to non-string (int) type')
            obj.__fields_values__[self.name] = value
        
class LongField(NumberField):
    
    def __init__(self, *args, **kwargs):
        super(LongField,self).__init__(*args, **kwargs)

    def set_value(self,obj,value):
        if value is None:
            obj.__fields_values__[self.name] = None
        else:
            if not isinstance(value,long):
                raise TypeError('Cant set LongField to non-string (long) type')
            obj.__fields_values__[self.name] = value
        
class FloatField(NumberField):
    
    def __init__(self, *args, **kwargs):
        super(FloatField,self).__init__(*args, **kwargs)
        
    def set_value(self,obj,value):
        if value is None:
            obj.__fields_values__[self.name] = None
        else:
            if not isinstance(value,float):
                raise TypeError('Cant set FloatField to non-string (float) type')
            obj.__fields_values__[self.name] = value
        
class BooleanField(BaseField):

    def __init__(self, *args, **kwargs):
        super(BooleanField,self).__init__(*args, **kwargs)
        
    def set_value(self,obj,value):
        if value is None:
            obj.__fields_values__[self.name] = None
        else:
            if not isinstance(value,bool):
                raise TypeError('Cant set BooleanField to non-string (bool) type')
            obj.__fields_values__[self.name] = value
        
class DateTimeField(BaseField):
        
    def __init__(self, *args, **kwargs):
        super(DateTimeField,self).__init__(*args, **kwargs)
        
    def set_value(self,obj,value):
        if value is None:
            obj.__fields_values__[self.name] = None
        else:
            if not isinstance(value,datetime.datetime):
                raise TypeError('Cant set DateTimeField to non-string (datetime.datetime) type')
            obj.__fields_values__[self.name] = value
        
class ReferenceField(BaseField):

    def __init__(self, *args, **kwargs):
        super(ReferenceField,self).__init__(*args, **kwargs)
        
        
