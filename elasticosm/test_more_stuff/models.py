from elasticosm.test_stuff.models import ExtendedModel
from elasticosm.models import ElasticModel, StringField

class A(ExtendedModel):
    
    field1 = StringField()
    
class B(ElasticModel):
    
    field2 = StringField()
    
class C(A,B):
    
    field3 = StringField()