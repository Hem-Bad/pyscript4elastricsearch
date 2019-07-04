
import time
import json
import logging
from logging.handlers import RotatingFileHandler
from logging import Formatter
from datetime import datetime
import sys
import argparse
from Reindexer import Reindexer


def init_logger(log_file, max_bytes, backup_count, verbose):
    logger = logging.getLogger("main")
    logger.setLevel(logging.DEBUG)

    log_file_handler = RotatingFileHandler(
        filename=log_file, maxBytes=max_bytes, backupCount=backup_count)
    log_file_handler.setLevel(logging.DEBUG)
    log_file_handler.setFormatter(
        Formatter("%(asctime)-15s [%(levelname)s] - %(message)s"))

    stream_handler = logging.StreamHandler(sys.stdout)
    if verbose:
        stream_handler.setLevel(logging.DEBUG)
    else:
        stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(
        Formatter("%(asctime)-15s [%(levelname)s] - %(message)s"))

    logger.addHandler(log_file_handler)
    logger.addHandler(stream_handler)

    return logger


if __name__ == '__main__':
    LOGFILE_MAX_BYTES = 1024 * 1024 * 1024  # 25 MB
    LOGFILE_BACKUP_COUNT = 500

    argparser = argparse.ArgumentParser(
        description='Reindex Elasticsearch', conflict_handler='resolve')

    # Elasticsearch parameters
    argparser.add_argument('-m', '--mapping-file',
                           help="File with JSON mapping", required=True)
    argparser.add_argument('-h', '--host', default='localhost',
                           help="Elasticsearch host (default localhost)")
    argparser.add_argument('-p', '--port', default='9200',
                           help="Elasticsearch port (default 9200)")
    argparser.add_argument(
        '-a', '--alias', help="Alias for the index", required=True)
    argparser.add_argument('-s', '--source-index',
                           help="Source index", required=True)
    argparser.add_argument('-t', '--target-index',
                           help="Target index (default: source_index_[YYMMDDhhmmss])")
    argparser.add_argument("-y", "--source-type",
                           help="Source type", required=True)
    argparser.add_argument("--target-type", help="Target type")

    # Logging parameters
    argparser.add_argument("--log-file", default="log/reindex.log")
    argparser.add_argument("--max-bytes", type=long,
                           default=LOGFILE_MAX_BYTES, help="Max file size per log file")
    argparser.add_argument("--backup-count", type=int,
                           default=LOGFILE_BACKUP_COUNT)

    # Misc params
    argparser.add_argument(
        "-v", "--verbose", action='store_true', help="Verbose")
    group = argparser.add_mutually_exclusive_group()
    group.add_argument("-i", "--interactive", action='store_true',
                       help="Confirm before taking action")
    group.add_argument("--dry-run", action='store_true', default=False,
                       help="Dry run. Reindexing will not be done if this option is set.")

    args = argparser.parse_args()

    if args.target_index == None:
        args.target_index = args.source_index + \
            "_" + datetime.now().strftime('%Y%m%d%H%M%S')
    if args.target_type == None:
        args.target_type = args.source_type

    reindexer = Reindexer(
        args.host, args.port, args.mapping_file,
        args.alias, args.source_index, args.target_index,
        args.source_type, args.target_type
    )

    logger = init_logger(args.log_file, args.max_bytes,
                         args.backup_count, args.verbose)
    logger.info("Elasticsearch host: " + reindexer.getHost())
    logger.info("Source index: " + reindexer.getSourceIndex())
    logger.info("Target index: " + reindexer.getTargetIndex())
    logger.info("Alias: " + reindexer.getAlias())
    logger.info("Source index type: " + reindexer.getSourceType())
    logger.info("Target index type: " + reindexer.getTargetType())

    if args.interactive:
        args.dry_run = False if (
            raw_input("\nContinue? (y/n): ") == 'y') else True
        logger.debug("User confirmation to proceed: " +
                     ('no' if args.dry_run else 'yes'))

    try:
        reindexer.create_mapping()
        if args.dry_run:
            logger.info("Mapping body -- " +
                        json.dumps(reindexer.getMapping(), indent=2))
        else:
            reindexer.reindex()
    except Exception, e:
        logger.error(e)

    logger.info("Exiting")
