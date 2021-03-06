from ceilometer_local_lib import make_sql_query_from_filter
from ceilometer_local_lib import PoolConnection
from ceilometer_local_lib import Object


def get_samples(sample_filter, limit=None):
    """Return an iterable of model.Sample instances.

   :param sample_filter: Filter.
   :param limit: Maximum number of results to return.
   """
    if limit == 0:
        return []
    query = ("SELECT sources.name as source_name, meters.name as meter_name,"
             " meters.type, meters.unit, samples.volume,"
             " users.uuid as user_id, projects.uuid as projects_id,"
             " resources.resource_id, samples.message_id,"
             " samples.message_signature, samples.recorded_at,"
             " samples.metadata, samples.timestamp"
             " FROM samples"
             " JOIN meters ON samples.meter_id = meters.id"
             " JOIN users ON samples.user_id = users.id"
             " JOIN projects ON samples.project_id = projects.id"
             " JOIN resources ON samples.resource_id = resources.id"
             " JOIN sources ON samples.source_id = sources.id")
    query, values = make_sql_query_from_filter(query, sample_filter, limit)
    query += ";"
    # print query
    # print values
    with PoolConnection() as cur:
        cur.execute(query, values)
        resp = cur.fetchall()
    print resp


sample_filter = Object
sample_filter.meter = 'cpu'
sample_filter.source = 'openstack'
sample_filter.start = None
sample_filter.start_timestamp_op = None
sample_filter.end = None
sample_filter.end_timestamp_op = None
sample_filter.user = None
sample_filter.project = None
sample_filter.resource = None
sample_filter.message_id = None
sample_filter.metaquery = {'metadata.status': 'active'}

get_samples(sample_filter)
