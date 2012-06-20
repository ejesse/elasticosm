elasticosm
==========

Python "OSM" (Object-Search Mapper) for Elastic Search

What's an "OSM"? Mostly it's wordplay on "ORM," but it does serve to illustrate that Elasticosm isn't really an ORM or a search API. It allows you to create models that are stored in Elastic Search and treat them similarly to (but not exactly like) ORM style models. You can create, update, delete, perform exact match filters and have references between models.

This is extremely alpha and in many ways an experiment to see if the performance and scalability of ES can be leveraged with the programmatic ease of a database API.