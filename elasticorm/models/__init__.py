from elasticorm.models.base import BaseElasticModel
from elasticorm.models.fields import BooleanField, DateTimeField, \
    FloatField, IntField, LongField, ReferenceField, StringField, SearchField, AutoDateTimeField, CreationDateTimeField, ReferenceField
import datetime

class ElasticModel(BaseElasticModel):
    
    created_date = CreationDateTimeField()
    last_modified_date = AutoDateTimeField()