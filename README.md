# Elasticsearch Wrapper
A tiny wrapper for [Elasticsearch](http://elasticsearch-py.readthedocs.io/en/master/index.html), supporting common operations with friendly interfaces.

## Usage
 Configure the client:
```
>> hosts = [127.0.0.1, ]
>> es = ElasticSearchEx(hosts)
>> print ('Instance display format: ' + str(es))
>> Instance display format: <Servers: 127.0.0.1, Version: 2.4.0, Status: connected>
```

* Create a new index for documents:
```
>> es.create_index(doc_index='ex', shard_num=3, replica_num=0)
>> True
```
Warning: the index, if exists, will be deleted automatically.

* Add new documents
```
>> data = [{'title': 'Flask Web Development', 
            'authors': 'Miguel Grinbery', 
            'date': '2015'}, 
            {'title': 'Learning Python', 
            'authors': 'Mark Lutz', 
            'date': '2016'},
            {'title': 'Python Programming', 
            'authors': 'Mark Lutz', 
            'date': '2014'}]
>> es.add_doc(doc_index='ex', doc_type='books', data_body=data)
>> (3, 0) # number of successfully and failed executed actions
```

* Get documents' id by query
```
>> query_body = '{"query":{"match_all":{}}}'
>> es.get_doc_id(doc_index=doc_index, doc_type=doc_type, query_body=query_body)
>> <generator object get_doc_id at 0x01EBE760>
>> list(es.get_doc_id(doc_index=doc_index, doc_type=doc_type, query_body=query_body))
>> [u'0', u'2', u'1']
```

* Get the number of matches for given query
```
>> es.get_count(doc_index='ex', doc_type='books', query_body='{"query":{"match_all":{}}}')
>> 3
```

* Delete documents by query
```
>> query_body = '{"query":{"match_phrase":{"title": "Flask Web Development"}}}'
>> es.delete_doc(doc_index=doc_index, doc_type=doc_type, query_body=query_body)
>> (1, 0)
```

* Update documents by query
```
>> data = {'date': '2016', 'update':'2016-11-22'}
>> query_body ='{"query":{"match_phrase":{"title": "Learning Python"}}}'
>> es.update_doc(doc_index=doc_index, doc_type=doc_type, data_body=data, query_body=query_body)
>> (1, 0)
```

* Get documents by query
```
>> query_body = '{"query":{"match_all":{}}}'
>> es.get_doc(doc_index=doc_index, doc_type=doc_type, query_body=query_body)
>> <generator object get_doc at 0x01DA7BE8>
>> list(es.get_doc(doc_index=doc_index, doc_type=doc_type, query_body=query_body))
>> [{u'date': u'2014', u'title': u'Python Programming', u'authors': u'Mark Lutz'}, 
    {u'date': u'2016', u'title': u'Learning Python', u'update': u'2016-11-22', u'authors': u'Mark Lutz'}]
```

* Delete Index
```
>> es.delete_index(doc_index='ex')
>> True
```
