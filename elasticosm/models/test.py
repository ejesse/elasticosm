from elasticosm.models import *


class TestModel(ElasticModel):
    
    name = StringField()
    
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