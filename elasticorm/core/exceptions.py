



class ElasticORMException(Exception):
    """Generic exception class."""
    def __init__(self, value):
        self.value = value
        self.message = value
    
    def __str__(self):
        return repr(self.value)