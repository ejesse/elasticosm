from elasticorm.models import *


class TestModel(ElasticModel):
    
    string_field = StringField()
    
    search_field = SearchField()

    int_field = IntField()
    
    long_field = LongField()
    
    float_field = FloatField()
    
    boolean_field = BooleanField()

    date_time_field = DateTimeField()
    
class TestModelToo(ElasticModel):

    name = StringField()
    
    ref = ReferenceField()