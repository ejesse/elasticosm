from elasticosm.models.base import BaseElasticModel
from elasticosm.models.fields import BooleanField, DateTimeField, FloatField, \
    IntField, LongField, SlugField, StringField, SearchField, \
    AutoDateTimeField, CreationDateTimeField, ReferenceField, ListField, \
    MultiReferenceField
import datetime


class ElasticModel(BaseElasticModel):

    created_date = CreationDateTimeField()
    last_modified_date = AutoDateTimeField()
