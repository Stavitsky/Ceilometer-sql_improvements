from ceilometer_local_lib import PoolConnection
from sample_generator import generate_random_sample
import psycopg2


def insert_prebill(data):
    counter_type = data['counter_type']

    if counter_type not in ['gauge', 'cumulative']:
        print("returned from counter_type checking")
        return

    if data['counter_name'] == 'memory' and \
       'state' in data['resource_metadata'] and \
       data['resource_metadata']['state'] not in ['active',
                                                  'resized',
                                                  'paused']:
        print("returned from counter_name checking")
        return

    if data['project_id'] is None or data['project_id'] == 'None':
        print("returned from project_id checking")
        return

    obj_id = data['resource_id']
    owner = obj_id  # should be ''
    volume = data['counter_volume']
    meter_name = data['counter_name'].replace('.', '-')

    if meter_name.startswith('instance'):
        meter_name += imageSuffix(data['resource_metadata']['image_ref'])
    elif meter_name.startswith('volume'):
        meter_name += volumeSuffix(data['resource_metadata']['volume_type'])
    elif meter_name in ['cpu', 'memory']:  # subcounters
        owner = obj_id
        obj_id = obj_id + ':' + meter_name
        if meter_name == 'cpu':
            volume /= 1000000000.0  # nanoseconds
    elif meter_name == 'image-size' and \
            'properties' in data['resource_metadata'] and \
            'instance_uuid' in data['resource_metadata']['properties']:
        owner = data['resource_metadata']['properties']['instance_uuid']
        if '_backup_' in data['resource_metadata']['properties']['name']:
            meter_name = 'backup-size'

    if 'create' in data['resource_metadata']['event_type']:
        event_type = 'start'
    elif 'delete' in data['resource_metadata']['event_type']:
        event_type = 'stop'
    else:
        event_type = 'exists'

    timestamp = data['timestamp'].replace(microsecond=0)
    tenant_key = data['project_id']
    value = volume if counter_type == 'cumulative' else 0

    query = ('INSERT INTO prebill (class, obj_key,'
             ' event_type, stamp_start, stamp_end, volume,'
             ' owner_key, tenant_key, counter_type,'
             ' last_value, last_timestamp) VALUES'
             ' ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11',
             ['text', 'uuid', 'text', 'timestamp', 'timestamp',
              'float', 'uuid', 'uuid', 'text', 'float', 'timestamp'])

    query = ('INSERT INTO prebill (class, obj_key,'
             ' event_type, stamp_start, stamp_end, volume,'
             ' owner_key, tenant_key, counter_type,'
             ' last_value, last_timestamp) VALUES'
             ' (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);')

    values = [meter_name, obj_id, event_type, timestamp, timestamp, value,
              owner, tenant_key, counter_type, volume, timestamp]

    print ("meter_name: {0}\nobj_id: {1}\nevent_type: {2}\ntimestamp: {3}".
           format(meter_name, obj_id, event_type, timestamp))
    print ("value: {0}\nowner: {1}\ntenant_key: {2}\ncounter_type: {3}".
           format(value, owner, tenant_key, counter_type))
    print ("volume: {0}".format(volume))

    with PoolConnection() as db:
        db.execute(query, values)

insert_prebill(generate_random_sample())
