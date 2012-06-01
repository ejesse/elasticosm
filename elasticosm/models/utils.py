

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
