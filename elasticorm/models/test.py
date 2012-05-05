from elasticorm.models.base import BaseElasticModel
from elasticorm.models.fields import StringField, TextField, IntField, LongField, \
    FloatField, BooleanField, DateTimeField



class TestModel(BaseElasticModel):
    
    string_field = StringField()
    
    text_field = TextField()

    int_field = IntField()
    
    long_field = LongField()
    
    float_field = FloatField()
    
    boolean_field = BooleanField()

    date_time_field = DateTimeField()
    
    type_name = 'test type name'
