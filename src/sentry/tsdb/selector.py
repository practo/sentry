from __future__ import absolute_import

import logging

from sentry.tsdb.base import BaseTSDB
from sentry.tsdb.snuba import SnubaTSDB


logger = logging.getLogger(__name__)


READ = 0
WRITE = 1


def single_model_argument(callargs):
    return set([callargs['model']])


def multiple_model_argument(callargs):
    return set(callargs['models'])


def dont_do_this(callargs):
    raise NotImplementedError('do not run this please')


method_specifications = {
    # method: (type, function(callargs) -> set[model])
    'get_range': (READ, single_model_argument),
    'get_sums': (READ, single_model_argument),
    'get_distinct_counts_series': (READ, single_model_argument),
    'get_distinct_counts_totals': (READ, single_model_argument),
    'get_distinct_counts_union': (READ, single_model_argument),
    'get_most_frequent': (READ, single_model_argument),
    'get_most_frequent_series': (READ, single_model_argument),
    'get_frequency_series': (READ, single_model_argument),
    'get_frequency_totals': (READ, single_model_argument),
    'incr': (WRITE, single_model_argument),
    'incr_multi': (WRITE, lambda callargs: set(model for model, key in callargs['items'])),
    'merge': (WRITE, single_model_argument),
    'delete': (WRITE, multiple_model_argument),
    'record': (WRITE, single_model_argument),
    'record_multi': (WRITE, lambda callargs: set(model for model, key, values in callargs['items'])),
    'merge_distinct_counts': (WRITE, single_model_argument),
    'delete_distinct_counts': (WRITE, multiple_model_argument),
    'record_frequency_multi': (WRITE, lambda callargs: set(model for model, data in callargs['requests'])),
    'merge_frequencies': (WRITE, single_model_argument),
    'delete_frequencies': (WRITE, multiple_model_argument),
    'flush': (WRITE, dont_do_this),
}

assert set(method_specifications) == BaseTSDB.__read_methods__ | BaseTSDB.__write_methods__, \
    'all read and write methods must have a specification defined'

model_backends = {
    # model: (read, write)
    model: ('redis', 'redis') if model not in SnubaTSDB.model_columns else ('snuba', 'dummy') for model in BaseTSDB.models
}


def selector_func(context, method, callargs):
    spec = method_specifications.get(method)
    if spec is None:
        return ['redis']  # default backend (possibly invoke base directly instead?)

    operation_type, model_extractor = spec
    backends = set([model_backends[model][operation_type] for model in model_extractor(callargs)])

    assert len(backends) == 1, 'request was not directed to a single backend'
    return list(backends)
