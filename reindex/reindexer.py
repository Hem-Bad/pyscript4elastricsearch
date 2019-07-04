from elasticsearch import Elasticsearch
from elasticsearch.client import IndicesClient
from elasticsearch import helpers
import json
import logging
from logging.handlers import RotatingFileHandler
from logging import Formatter
import sys


class Reindexer:

    "Elasticsearch reindexer"

    def __init__(self, host, port, mapping_file, alias, source_index, target_index, source_type, target_type):

        self.__host = host
        self.__port = port
        self.__mapping_file = mapping_file
        self.__alias = alias
        self.__source_index = source_index
        self.__target_index = target_index
        self.__source_type = source_type
        self.__target_type = target_type

    def __get_mappings(self):
        mapping = {self.__target_type: {}}
        fobj = open(self.__mapping_file)
        for line in fobj.readlines():
            if len(line.strip()) > 0:
                fields_mapping = json.loads(line)
                mapping[self.__source_type] = fields_mapping
                break
        return mapping

    def create_mapping(self):

        mapping = self.__get_mappings()

        self.__body = None
        if mapping[self.__target_type] is not None:
            self.__body = {"mappings": mapping}

    def reindex(self):

        elastic_client = Elasticsearch(
            [{"host": self.__host, "port": self.__port}])
        index_client = IndicesClient(elastic_client)

        # Create new index with necessory fields mapping
        # , master_timeout=10, timeout=10
        index_client.create(index=self.__target_index, body=self.__body)

        # reindexind data from source index to target index
        helpers.reindex(client=elastic_client, source_index=self.__source_index,
                        target_index=self.__target_index)

        # creating alias for target index
        alias = {'actions': []}
        # remove_action = {"remove": {"index": self.__source_index, "alias": self.__alias}}
        add_action = {
            "add": {"index": self.__target_index, "alias": self.__alias}}
        # alias['actions'].append(remove_action)
        alias['actions'].append(add_action)

        # deleteing the source index
        index_client.delete(index=self.__source_index)
        index_client.update_aliases(body=alias)

    def getHost(self):
        return self.__host

    def getSourceIndex(self):
        return self.__source_index

    def getTargetIndex(self):
        return self.__target_index

    def getAlias(self):
        return self.__alias

    def getSourceType(self):
        return self.__source_type

    def getTargetType(self):
        return self.__target_type

    def getMapping(self):
        return self.__body
