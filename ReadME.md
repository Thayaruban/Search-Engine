# Tamil Songs Search Engine

This Repository includes the frontend,backend implementation for a search query.
After configuring the elasticsearch, the sample search engine is used to try the query searches.


Directory Structure
---
```
 ├── analyzers : Custom filters (Stemmers/stoppingwords,synonyms)
 ├── corpus : Modified data from the pre-processed data from actual (corpus) 
 ├── templates : The resultpage for UI (of Flask)
 ├── app.py : Flask backend to have transaction with ElasticSearch APIs
 ├── preprocesspy : Python file corpus data to elasticsearch input data format
 ├── query.py : ElasticSearch search queries inclusive of advanced queries, aggreagtions and textmining
 ├── static: contain front-end files
```

Demo
---
* Install ElasticSearch
* Install packages `pip install -r requirements.txt`
* Add 'analyze' folder in config of Elasticsearch and add files from analyzers
* Run ElasticSearch
* Add index(uncomment indexing part if not manually added) and add data (`processed_songs_bulk.json`)
* Go to http://localhost/5000/
* Enter keyword for search


