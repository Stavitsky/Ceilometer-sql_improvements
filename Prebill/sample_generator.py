import random
import uuid
import datetime
import string
import pprint


def rand_counter_name():
    counter_names = ['disk.read.requests.rate',
                     'disk.read.requests',
                     'instance',
                     'cpu',
                     'disk.write.requests',
                     'instance:m1.tiny',
                     'disk.read.bytes.rate',
                     'disk.read.bytes',
                     'cpu_util',
                     'disk.write.requests.rate',
                     'disk.write.bytes.rate',
                     'disk.write.bytes',
                     'network.services.firewall',
                     'image',
                     'image.size',
                     'network.services.firewall.policy',
                     'memory',
                     'vcpus',
                     'disk.ephemeral.size',
                     'disk.root.size',
                     'network.outgoing.packets',
                     'disk.device.read.requests',
                     'disk.device.read.bytes',
                     'network.outgoing.bytes',
                     'disk.device.write.bytes',
                     'network.incoming.bytes',
                     'disk.device.write.requests',
                     'network.incoming.packets',
                     'network.outgoing.bytes.rate',
                     'network.incoming.bytes.rate',
                     'network.incoming.packets.rate',
                     'network.outgoing.packets.rate']
    return random.choice(counter_names)


def rand_user_id():
    return str(uuid.uuid4())


def rand_string(length):
    return (''.join(random.choice(string.lowercase + string.digits)
                    for i in range(length)))


def rand_message_signature():
    return rand_string(64)


def rand_resource_id():
    return str(uuid.uuid4())


def rand_message_id():
    return str(uuid.uuid4())


def rand_source():
    # add some new if exist
    sources = ['openstack']
    return random.choice(sources)


def rand_counter_unit():
    counter_units = ['packet',
                     'policy',
                     'request/s',
                     'MB',
                     'vcpu',
                     'firewall',
                     'ns',
                     'request',
                     'B',
                     'instance',
                     'GB',
                     'B/s',
                     'image',
                     'packet/s',
                     '%']

    return random.choice(counter_units)


def rand_counter_volume():
    return random.randrange(10000000, 20000000)


def rand_project_id():
    # return rand_string(32)
    return str(uuid.uuid4())


def random_resource_metadata():
    return {'status': '',
            'name': '',
            'deleted': random.choice([True, False]),
            'container_format': random.choice(['bare', 'another_format']),
            'created_at': datetime.datetime.utcnow(),
            'disk_format': random.choice(['qcow2']),
            'update_at': datetime.datetime.utcnow(),
            'properties': {},
            'protected': random.choice([True, False]),
            'checksum': '133eae9fb1c98f45894a4e60d8736619',
            'min_disk': random.randrange(0, 1024),
            'is_public': random.choice([True, False]),
            'deleted_at': random.choice([True, False]),
            'min_ram': random.randrange(0, 1024),
            'size': random.randrange(10000000, 20000000),
            'event_type': random.choice(['create', 'delete'])
            }


def random_counter_type():
    counter_types = ['gauge', 'cmulative']
    return random.choice(counter_types)


def generate_random_sample():
    return {'counter_name': rand_counter_name(),
            'user_id': rand_user_id(),
            'message_signature': rand_message_signature(),
            'timestamp': datetime.datetime.utcnow(),
            'resource_id': rand_resource_id(),
            'message_id': rand_message_id(),
            'source': rand_source(),
            'counter_unit': rand_counter_unit(),
            'counter_volume': rand_counter_volume(),
            'project_id': rand_project_id(),
            'resource_metadata': random_resource_metadata(),
            'counter_type': random_counter_type()}


def print_randome_samples(number):
    for i in range(0, number):
        # print("\n Random generated sample #{}:\n".format(i+1))
        # pprint.pprint(generate_random_sample())
        print str(generate_random_sample())
        # generate_random_sample()


