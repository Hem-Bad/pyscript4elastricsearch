# elasticsearch-reindex
Python script to reindex elasticsearch

Dependency: elasticsearch

    pip install elasticsearch

Usage:

    $ python reindex.py [--help] [-h HOST] -f FILE -a ALIAS -s SOURCE_INDEX
                  [-t TARGET_INDEX] -y SOURCE_TYPE [--target-type TARGET_TYPE]
                  [-i] [-v] [--dry-run]


    optional arguments:
      --help                show this help message and exit
      -h HOST, --host HOST  Elasticsearch host
      -f FILE, --file FILE  File with JSON mapping
      -a ALIAS, --alias ALIAS
                        Alias for the index
      -s SOURCE_INDEX, --source-index SOURCE_INDEX
                        Source index
      -t TARGET_INDEX, --target-index TARGET_INDEX
                        Target index (default: source_index_[YYMMDDhhmmss]
      -y SOURCE_TYPE, --source-type SOURCE_TYPE
                        Source type
      --target-type TARGET_TYPE
                        Target type
      -i, --interactive     Confirm before taking action
      -v, --verbose         Verbose
      --dry-run             To do a dry run.