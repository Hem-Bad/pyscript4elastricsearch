# How to Find and Remove Duplicate Documents in Elasticsearch

Many systems that drive data into Elasticsearch will take advantage of Elasticsearch’s auto-generated id values for newly inserted documents. However, if the data source accidentally sends the same document to Elasticsearch multiple times, and if such auto-generated _id values are used for each document that Elasticsearch inserts, then this same document will be stored multiple times in Elasticsearch with different _id values. If this occurs then it may be necessary to find and remove such duplicates. Therefore, in this blog post we cover how to detect and remove duplicate documents from Elasticsearch by (1) using Logstash, or (2) using custom code written in Python.

# Example document structure
For the purposes of this blog post, we assume that the documents in the Elasticsearch cluster have the following structure. This corresponds to a dataset that contains documents representing stock market trades.
```
   {
      "_index": "stocks",
      "_type": "doc",
      "_id": "6fo3tmMB_ieLOlkwYclP",
      "_version": 1,
      "found": true,
      "_source": {
        "CAC": 1854.6,
        "host": "Alexanders-MBP",
        "SMI": 2061.7,
        "@timestamp": "2017-01-09T02:30:00.000Z",
        "FTSE": 2827.5,
        "DAX": 1527.06,
        "time": "1483929000",
        "message": "1483929000,1527.06,2061.7,1854.6,2827.5\r",
        "@version": "1"
      }
    }
```
Given this example document structure, for the purposes of this blog we arbitrarily assume that if multiple documents have the same values for the ```["CAC", "FTSE", "SMI"]``` fields that they are duplicates of each other.

Using Logstash for deduplication of Elasticsearch documents
Logstash may be used for detecting and removing duplicate documents from an Elasticsearch index. This technique is described in this blog about handling duplicates with Logstash, and this section demonstrates a concrete example which applies this approach.

In the example below I have written a simple Logstash configuration that reads documents from an index on an Elasticsearch cluster, then uses the fingerprint filter to compute a unique _id value for each document based on a hash of the ["CAC", "FTSE", "SMI"] fields, and finally writes each document back to a new index on that same Elasticsearch cluster such that duplicate documents will be written to the same _id and therefore eliminated.

Additionally, with minor modifications, the same Logstash filter could also be applied to future documents written into the newly created index in order to ensure that duplicates are removed in near real-time. This could be accomplished by changing the input section in the example below to accept documents from your real-time input source rather than pulling documents from an existing index.

Be aware that using custom _id values (i.e. an _id that is not generated by Elasticsearch) will have some impact on the write performance of your index operations.

Also, it is worth noting that depending on the hash algorithm used, this approach may theoretically result in a non-zero number of hash collisions for the _id value, which could theoretically result in two non-identical documents being mapped to the same _id, thus causing one of these documents to be lost. For most practical cases, the probability of a hash collision is likely very low. A detailed analysis of different hash functions is beyond the scope of this blog, but the hash function used in the fingerprint filter should be considered carefully as it will have an impact on ingest performance and on the number of hash collisions.

A simple Logstash configuration to dedupe an existing index using the fingerprint filter is given below.
```
input {
  # Read all documents from Elasticsearch 
  elasticsearch {
    hosts => "localhost"
    index => "stocks"
    query => '{ "sort": [ "_doc" ] }'
  }
}

# This filter has been updated on February 18, 2019
filter {
    fingerprint {
        key => "1234ABCD"
        method => "SHA256"
        source => ["CAC", "FTSE", "SMI"]
        target => "[@metadata][generated_id]"
        concatenate_sources => true # <-- New line added since original post date
    }
}
output {
    stdout { codec => dots }
    elasticsearch {
        index =>"stocks_after_fingerprint"
        document_id => "%{[@metadata][generated_id]}"
    }
}
```
## Custom Python script for Elasticsearch documents deduplication

### A memory-efficient approach
If Logstash is not used, then deduplication may be efficiently accomplished with a custom python script. For this approach, we compute the hash of the ["CAC", "FTSE", "SMI"] fields that we have defined to uniquely identify a document. We then use this hash as a key in a python dictionary, where the associated value of each dictionary entry will be an array of the _ids of the documents that map to the same hash.

If more than one document has the same hash, then the duplicate documents that map to the same hash can be deleted. Alternatively, if you are concerned about the possibility of hash collisions, then the contents of documents that map to the same hash can be examined to see if the documents are really identical, and if so, then duplicates can be eliminated.

### Detection algorithm analysis
For a 50GB index, if we assume that the index contains documents with an average size of 0.4 kB, then there would be 125 million documents in the index. In this case the amount memory required for storing the deduplication data structures in memory when using a 128-bit md5 hash would be on the order of 128 bits x 125 Million = 2GB of memory, plus the 160 bit _ids will require another 160 bits x 125 Million = 2.5 GB of of memory. This algorithm will therefore require on the order of 4.5GB RAM to keep all relevant data structures in memory. This memory footprint can be dramatically reduced if the approach discussed in the following section can be applied.

### Algorithm enhancement
In this section we present an enhancement to our algorithm to reduce memory usage as well as to continuously remove new duplicate documents.

If you are storing time-series data, and you know that duplicate documents will only occur within some small amount of time of each other, then you may be able to improve this algorithm's memory footprint by repeatedly executing the algorithm on a subset of the documents in the index, with each subset corresponding to a different time window. For example, if you have a years worth of data then you could use range queries on your datetime field (inside a filter context for best performance) to step through your data set one week at a time. This would require that the algorithm is executed 52 times (once for each week) -- and in this case, this approach would reduce the worst-case memory footprint by a factor of 52.

In the above example, you may be concerned about not detecting duplicate documents that span between weeks. Let’s assume that you know that duplicate documents cannot occur more than 2 hours apart. Then you would need to ensure that each execution of the algorithm includes documents that overlap by 2 hours with the last set of documents analyzed by the previous execution of the algorithm. For the weekly example, you would therefore need to query 170 hours (1 week + 2 hours) worth of time-series documents to ensure that no duplicates are missed.

If you wish to periodically clear out duplicate documents from your indices on an on-going basis, you can execute this algorithm on recently received documents. The same logic applies as above -- ensure that recently received documents are included in the analysis along with enough of an overlap with slightly older documents to ensure that duplicates are not inadvertently missed.

Python code to detect duplicate documents
The following code demonstrates how documents can can be efficiently evaluated to see if they are identical, and then eliminated if desired. However, in order to prevent accidental deletion of documents, in this example we do not actually execute a delete operation. Such functionality would be straightforward to include.

The code to deduplicate documents from Elasticsearch can also be found on github.
```
!/usr/local/bin/python3
import hashlib
from elasticsearch import Elasticsearch
es = Elasticsearch(["localhost:9200"])
dict_of_duplicate_docs = {}
The following line defines the fields that will be
used to determine if a document is a duplicate
keys_to_include_in_hash = ["CAC", "FTSE", "SMI"]
```
Process documents returned by the current search/scroll
def populate_dict_of_duplicate_docs(hits):
    for item in hits:
        combined_key = ""
        for mykey in
Read More

# Conclusion
In this blog post we have demonstrated two methods for deduplication of documents in Elasticsearch. The first method uses Logstash to remove duplicate documents, and the second method uses a custom Python script to find and remove duplicate documents.

If you have any questions about deduplication of Elasticsearch documents, or any other Elasticsearch-related topics, have a look at our Discuss forums for valuable insights and information.