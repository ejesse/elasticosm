from dateutil import parser
from elasticorm.models.internal_fields import BaseField, NumberField
from elasticorm.models.base import BaseElasticModel
import datetime
import pytz

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
        
class SearchField(StringField):

    def __init__(self, *args, **kwargs):
        super(SearchField,self).__init__(*args, **kwargs)
        
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
            if isinstance(value,str):
                value = unicode(value)
            if isinstance(value,unicode):
                try:
                    v = parser.parse(value)
                    obj.__fields_values__[self.name] = v
                except ValueError, e:
                    raise TypeError('Unable to set DateTimeField to parsed value of %s due to %s' % (value, e))
            elif not isinstance(value,datetime.datetime):
                raise TypeError('Cant set DateTimeField to non-string (datetime.datetime) type')
            else:
                obj.__fields_values__[self.name] = value
                
class AutoDateTimeField(DateTimeField):
    
    def on_save(self,obj):
        d = datetime.datetime.utcnow()
        d = d.replace(tzinfo=pytz.utc)
        self.set_value(obj, d)
        
class CreationDateTimeField(DateTimeField):
        
    def on_save(self,obj):
        if obj.id is None:
            d = datetime.datetime.utcnow()
            d = d.replace(tzinfo=pytz.utc)
            self.set_value(obj, d)
        
        
class ReferenceField(BaseField):

    def __init__(self, *args, **kwargs):
        super(ReferenceField,self).__init__(*args, **kwargs)
        
    def set_value(self,obj,value):
        if isinstance(value,BaseElasticModel):
            if value.id is None:
                next_int = len(obj.__reference_cache__.keys()) + 1
                temp_id = 'TEMP_ID_%s' % (next_int)
                obj.__reference_cache__[temp_id] = value 
                obj.__fields_values__[self.name] = temp_id
            else:
                obj.__fields_values__[self.name] = value.id
                obj.__reference_cache__[value.id] = value
        else:
            obj.__fields_values__[self.name] = value

    def get_value(self, obj):
        value_id = obj.__fields_values__[self.name]
        if not obj.__reference_cache__.has_key(value_id):
            referenced_object = BaseElasticModel.get(id=value_id)
            obj.__reference_cache__[value_id] = referenced_object
        return obj.__reference_cache__[value_id]
    
    def on_save(self,obj):
        if obj.__fields_values__[self.name] is not None:
            try:
                temp_id = obj.__fields_values__[self.name]
                temp_id.index("TEMP_ID")
                referenced_object = obj.__reference_cache__[temp_id]
                referenced_object.save()
                obj.__fields_values__[self.name]=referenced_object.id
                obj.__reference_cache__[referenced_object.id] = referenced_object
                # remove the entry with the temp id
                obj.__reference_cache__.pop(temp_id)
            except ValueError:
                ## no extra work needed, either it's empty or it's an object that's already saved
                pass 
        
        
        
