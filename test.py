#encoding=utf-8

# ---------------------------------------------------------libraries--------------------------------------------------------
# Standard library
import pprint

# Third-party libraries
from elasticsearchex import ElasticSearchEx

# User define module


# --------------------------------------------------------Global Variables--------------------------------------------------
hosts = ["127.0.0.1", ]
doc_index = 'ex'
doc_type = 'books'

# -------------------------------------------------------Class ElasticSearchEx----------------------------------------------
# Configure the client:
es = ElasticSearchEx(hosts)
print('Instance display format: ' + str(es) + '\n')

# Create a new index for documents
result = es.create_index(doc_index=doc_index, shard_num=3, replica_num=0)
print('New Index: ' + str(result) + '\n')

# Add new documents
data = [{'title': 'Flask Web Development', 
        'authors': 'Miguel Grinbery',
        'date': '2015'}, 
        {'title': 'Learning Python', 
        'authors': 'Mark Lutz', 
        'date': '2012'},
        {'title': 'Python Programming', 
        'authors': 'Mark Lutz', 
        'date': '2014'}]
result = es.add_doc(doc_index=doc_index, doc_type=doc_type, data_body=data)
print('Add documents: ' + str(result) +'\n')

# Get documents' id by query
query_body = '{"query":{"match_all":{}}}'
result = es.get_doc_id(doc_index=doc_index, doc_type=doc_type, query_body=query_body)
print('Get documents id: ' + str(result))
print('Get documents id (list): ' + str(list(result)) + '\n')

# Get the number of matches for given query
query_body='{"query":{"match_all":{}}}'
result = es.get_count(doc_index=doc_index, doc_type=doc_type, query_body=query_body)
print('Number of matches: ' + str(result) + '\n')

# Delete documents by query
query_body = '{"query":{"match_phrase":{"title": "Flask Web Development"}}}'
result = es.delete_doc(doc_index=doc_index, doc_type=doc_type, query_body=query_body)
print('Detete documents: ' +str(result) + '\n')

# Update documents by query
data = {'date': '2016', 'update':'2016-11-22'}
query_body ='{"query":{"match_phrase":{"title": "Learning Python"}}}'
result = es.update_doc(doc_index=doc_index, doc_type=doc_type, data_body=data, query_body=query_body)
print('Update documents: ' + str(result) + '\n')

# Get documents by query
query_body = '{"query":{"match_all":{}}}'
result = es.get_doc(doc_index=doc_index, doc_type=doc_type, query_body=query_body)
print('Get documents: ' +str(result))
print('Get documents (list): ')
pprint.pprint(list(result), indent=2)
print('\n')

# Delete index
result = es.delete_index(doc_index='ex')
print('Delete index: ' +str(result) + '\n')