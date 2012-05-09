from elasticorm.models.internal_fields import BaseField, NumberField


class StringField(BaseField):
    
    def __init__(self, *args, **kwargs):
        super(StringField,self).__init__(*args, **kwargs)
        
class TextField(StringField):

    def __init__(self, *args, **kwargs):
        super(TextField,self).__init__(*args, **kwargs)
        
class IntField(NumberField):
    
    def __init__(self, *args, **kwargs):
        super(IntField,self).__init__(*args, **kwargs)

class LongField(NumberField):
    
    def __init__(self, *args, **kwargs):
        super(LongField,self).__init__(*args, **kwargs)

class FloatField(NumberField):
    
    def __init__(self, *args, **kwargs):
        super(FloatField,self).__init__(*args, **kwargs)
        
class BooleanField(BaseField):

    def __init__(self, *args, **kwargs):
        super(BooleanField,self).__init__(*args, **kwargs)
        
class DateTimeField(BaseField):
        
    def __init__(self, *args, **kwargs):
        super(DateTimeField,self).__init__(*args, **kwargs)
        
class ReferenceField(BaseField):

    def __init__(self, *args, **kwargs):
        super(ReferenceField,self).__init__(*args, **kwargs)
        
        
