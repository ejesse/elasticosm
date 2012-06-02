elasticosm
==========

Python "OSM" (Object-Search Mapper) for Elastic Search

What's an "OSM"? It's a play on ORM. Elasticosm isn't really an ORM or a search API. It allows you to create models that are stored in Elastic Search and treat them similarly (but not exactly like) to ORM style models. You can create, update, delete, perform exact match filters and have references.

This is extremely alpha and in many ways an experiment to see if the performance and scalability of ES can be leveraged with the programmatic ease of a database API.