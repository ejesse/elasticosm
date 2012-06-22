import re

_slugify_strip_re = re.compile(r'[^\w\s-]')
_slugify_hyphenate_re = re.compile(r'[-\s]+')

def slugify(value):
    """
    Normalizes string, converts to lowercase, removes non-alpha characters,
    and converts spaces to hyphens.
    
    From Django's "django/template/defaultfilters.py".
    """
    import unicodedata
    if not isinstance(value, unicode):
        value = unicode(value)
    value = unicodedata.normalize('NFKD', value).encode('ascii', 'ignore')
    value = unicode(_slugify_strip_re.sub('', value).strip().lower())
    return _slugify_hyphenate_re.sub('-', value)

def get_all_base_classes(cls):

    bases = set()
    work = [cls]
    while work:
        current = work.pop()
        for parent in current.__bases__:
            if parent not in bases:
                bases.add(parent)
                work.append(parent)
    return bases

def is_elastic_model(cls):
    all_bases = get_all_base_classes(cls)
    from elasticosm.models import ElasticModel
    for b in all_bases:
        if b == ElasticModel:
            return True
    return False
