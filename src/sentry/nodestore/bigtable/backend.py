from __future__ import absolute_import, print_function

from six import text_type
from google.cloud import bigtable
from simplejson import JSONEncoder, _default_decoder

from sentry.nodestore.base import NodeStorage
from sentry.utils.cache import memoize

# Cache an instance of the encoder we want to use
json_dumps = JSONEncoder(
    separators=(',', ':'),
    skipkeys=False,
    ensure_ascii=True,
    check_circular=True,
    allow_nan=True,
    indent=None,
    encoding='utf-8',
    default=None,
).encode

json_loads = _default_decoder.decode


class BigtableNodeStorage(NodeStorage):
    """
    A Bigtable-based backend for storing node data.

    >>> BigtableNodeStorage(
    ...     project='some-project',
    ...     instance='sentry',
    ...     table='nodestore',
    ... )
    """

    bytes_per_column = 1024 * 1024 * 10
    max_size = 1024 * 1024 * 100
    columns = [text_type(i).encode() for i in range(max_size / bytes_per_column)]
    column_family = b'x'

    def __init__(self, project=None, instance='sentry', table='nodestore', **kwargs):
        self.project = project
        self.instance = instance
        self.table = table
        self.options = kwargs
        super(BigtableNodeStorage, self).__init__()

    @memoize
    def connection(self):
        return (
            bigtable.Client(project=self.project, **self.options)
            .instance(self.instance)
            .table(self.table)
        )

    def delete(self, id):
        row = self.connection.row(id)
        row.delete()
        row.commit()

    def get(self, id):
        row = self.connection.read_row(id)
        if row is None:
            return None
        data = []
        columns = row.cells[self.column_family]
        for column in self.columns:
            try:
                data.append(columns[column][0].value)
            except KeyError:
                break
        return json_loads(b''.join(data))

    def set(self, id, data):
        row = self.connection.row(id)
        # Call to delete is just a state mutation,
        # and in this case is just used to clear all columns
        # so the entire row will be replaced. Otherwise,
        # if an existing row were mutated, and it took up more
        # than one column, it'd be possible to overwrite
        # beginning columns and still retain the end ones.
        row.delete()
        cells = 0
        data = json_dumps(data)
        for idx, column in enumerate(self.columns):
            offset = idx * self.bytes_per_column
            chunk = data[offset:offset + self.bytes_per_column]
            if len(chunk) == 0:
                break
            row.set_cell(self.column_family, column, chunk)
            cells += 1
        row.commit()

    def cleanup(self, cutoff_timestamp):
        raise NotImplementedError

    def bootstrap(self):
        table = (
            bigtable.Client(project=self.project, admin=True, **self.options)
            .instance(self.instance)
            .table(self.table)
        )
        if table.exists():
            return

        table.create(column_families={
            # TODO: GC policy
            self.column_family: None,
        })
