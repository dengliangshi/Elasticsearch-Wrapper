#encoding=utf-8

# ---------------------------------------------------------libraries--------------------------------------------------------
# Standard library


# Third-party libraries
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from elasticsearch import TransportError

# User define module


# --------------------------------------------------------Global Variables--------------------------------------------------


# ------------------------------------------------------Class ElasticSearchEx-----------------------------------------------
class ElasticSearchEx(object):
    """A simple wrapper for Elasticsearch.
    """
    def __init__(self, server_hosts=None, **kwargs):
        """This class is responsible for building up a connection to an ElasticSearch node and returning query result
        according to given query body.
        :Param server_hosts: list of server host names
        """
        self.es = None  # ElasticSearch client
        self.server_hosts = server_hosts
        if self.server_hosts:  # connect to ElasticSearch server
            self.connect(server_hosts, **kwargs)  

    def __repr__(self):
        """Instance's dispaly format.
        """
        if self.es is not None:
            return '<Servers: %s, Version: %s, Status: connected>' % (', '.join(self.server_hosts), 
                self.es.info()['version']['number'])
        else:
            return '<Servers: %s, Status: disconnected>' % ', '.join(self.server_hosts)

    def connect(self, server_hosts, **kwargs):
        """Build up connection to an ElasticSearch node if not.
        :Param server_hosts: list of server host names
        """
        if self.es is None:  # connect to ElasticSearch server
            self.es = Elasticsearch(server_hosts, retry_on_timeout=True, **kwargs)

    def create_index(self, doc_index, shard_num=5, replica_num=1):
        """Create new index.
        :Param doc_index: the name of the new index to be created
        :Param shard_num: the number of shards, default is 5
        :Param replica_num: the number of replica, default is 1
        """
        request_body = {  # The configuration for the index
            'settings' : {
                'number_of_shards': shard_num,
                'number_of_replicas': replica_num}
            }
        if self.es.indices.exists(doc_index):  # delete the index if exists
            self.es.indices.delete(index = doc_index)
        return self.es.indices.create(index = doc_index, body = request_body)['acknowledged']

    def delete_index(self, doc_index):
        """Delete an index.
        :Param doc_index: the name of the index to be deleted
        """
        if self.es.indices.exists(doc_index):  # delete the index if exists
            return self.es.indices.delete(index = doc_index)['acknowledged']

    def get_count(self, doc_index, doc_type, query_body):
        """Get the number of matches for given query.
        :Param doc_index: the index of document
        :Param doc_type: the type of document
        :Param query_body: the query DSL
        """
        try:
            return self.es.count(index=doc_index, doc_type=doc_type, 
                body=query_body)['count']
        except TransportError: # the document type does not exist
            return 0

    def get_doc_id(self, doc_index, doc_type, query_body):
        """Get the id of the target data.
        :Param doc_index: the name of index
        :Param doc_type: the type of target document
        :Param query_body: the query DSL
        """
        try:
            for result in helpers.scan(client=self.es, query=query_body, 
                index=doc_index, doc_type=doc_type):
                yield result.get('_id')
        except TransportError: pass # the document type does not exist

    def add_doc(self, doc_index, doc_type, data_body):
        """Add date into ElasticSearch.
        :Param doc_index: the name of documents' index
        :Param doc_type: the type of documents
        :Param data_body: new data to be added
        """
        try:
            max_id = max(self.get_doc_id(doc_index=doc_index, doc_type=doc_type,
                query_body='{"query":{"match_all":{}}}'))
        except ValueError:
            max_id = -1
        actions = [{
            '_op_type': 'index',
            '_index': doc_index,
            '_type': doc_type,
            '_id': max_id+index+1,
            '_source': data} for index, data in enumerate(data_body)]
        return helpers.bulk(client=self.es, actions=actions, stats_only=True, refresh=True)

    def delete_doc(self, doc_index, doc_type, query_body):
        """Delete data from ElasticSearch.
        :Param doc_index: the name of documents' index
        :Param doc_type: the type of the documents
        :Param query_body: query DSL for searching documents to be deleted
        """
        actions = [{
            '_op_type': 'delete',
            '_index': doc_index,
            '_type': doc_type,
            '_id': doc_id} for doc_id in self.get_doc_id(doc_index, doc_type, query_body)]
        return helpers.bulk(client=self.es, actions=actions, stats_only=True, refresh=True)

    def update_doc(self, doc_index, doc_type, query_body, data_body):
        """update documents in ElasticSearch.
        :Param doc_index: the name of documents' index
        :Param doc_type: the type of documents
        :Param query_body: query DSL for searching documents to be updated
        """
        actions = [{
            '_op_type': 'update',
            '_index': doc_index,
            '_type': doc_type,
            '_id': doc_id,
            'doc': data_body} for doc_id in self.get_doc_id(doc_index, doc_type, query_body)]
        return helpers.bulk(client=self.es, actions=actions, stats_only=True, refresh=True)

    def get_doc(self, doc_index, doc_type, query_body):
        """Get the query results for given query body using helpers.scan() method.
        -Param doc_index: the name of documents' index
        -Param doc_type: the type of target document
        -Param query_body: the query DSL
        """
        for result in helpers.scan(client=self.es, query=query_body, 
            index=doc_index, doc_type=doc_type):
            yield result.get('_source')
