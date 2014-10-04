import os
import logging
import logging.config

import yaml


def setup_logging(
        default_path='logging.yaml',
        default_level=logging.INFO,
        env_key='LOG_CFG'
):
    """Setup logging configuration

    """
    path = default_path
    value = os.getenv(env_key, None)
    if value:
        path = value
    if os.path.exists(path):
        with open(path, 'rt') as f:
            config = yaml.load(f.read())
        logging.config.dictConfig(config)
    else:
        logging.basicConfig(level=default_level)


def save_reults(results):
    logger = logging.getLogger('results')
    if isinstance(results, list):
        map(lambda x: logger.info(x), results)
    else:
        logger.info(results)


def next_term(event_num, term_length):
    alpha = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ'
    # create the format string
    foo = "{0:0" + str(term_length) + "}"
    # create a density string ("one every 100" etc.)
    density = pow(10, term_length)
    # create new id based on serial event number modulo density
    id = event_num % density
    # create the leading character based on id modulo 36 + rest of result
    result = alpha[id % 36] + foo.format(id)
    # the leading character (0-Z) is used to have a better lex spread than numbers only
    # and the rest of "result" dictates term density
    return result