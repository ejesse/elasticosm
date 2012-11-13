

class ReferenceObject(object):
    
    def __init__(self, instance_id=None, instance=None):
        self.instance=None
        self.instance_id=None
        if instance_id is not None:
            self.instance_id=instance_id
        if instance is not None:
            self.instance = instance
            if self.instance.id is not None:
                self.instance_id = self.instance.id
            
    def get_instance(self):
        if self.instance is None:
            from elasticosm.models import ElasticModel
            self.instance = ElasticModel.get(id=self.instance_id)
        return self.instance
    
    def __repr__(self):
        return self.get_instance()