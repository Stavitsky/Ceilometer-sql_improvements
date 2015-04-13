from ceilometer_local_lib import PoolConnection
from ceilometer_local_lib import Object
from psycopg2.extras import Json
from get_alarms import get_alarms
import datetime


def _row_to_alarm_model(row):
    return alarm_api_models.Alarm(alarm_id=row.alarm_id,
                                  enabled=row.enabled,
                                  type=row.type,
                                  name=row.name,
                                  description=row.description,
                                  timestamp=row.timestamp,
                                  user_id=row.user_id,
                                  project_id=row.project_id,
                                  state=row.state,
                                  state_timestamp=row.state_timestamp,
                                  ok_actions=row.ok_actions,
                                  alarm_actions=row.alarm_actions,
                                  insufficient_data_actions=(
                                      row.insufficient_data_actions),
                                  rule=row.rule,
                                  time_constraints=row.time_constraints,
                                  repeat_actions=row.repeat_actions)


def update_alarm(alarm):

    sql_subquery = ('SELECT alarm.alarm_id, alarm.enabled, alarm.type,'
                    ' alarm.name, alarm.description, alarm.timestamp,'
                    ' users.uuid as user_id, projects.uuid as project_ids,'
                    ' alarm.state, alarm.state_timestamp, alarm.ok_actions,'
                    ' alarm.alarm_actions, alarm.insufficient_data_actions,'
                    ' alarm.rule, alarm.time_constraints, alarm.repeat_actions'
                    ' FROM alarm'
                    ' LEFT JOIN users ON alarm.user_id = users.id'
                    ' LEFT JOIN projects ON alarm.project_id = projects.id'
                    ' WHERE alarm_id = %s'
                    )
    values = []
    sql_query = (' UPDATE alarm'
                 ' SET')

    if alarm['user_id']:
        user_id_q = ("SELECT id FROM users"
                     " WHERE uuid = %s")
        with PoolConnection() as cur:
            cur.execute(user_id_q, (alarm['user_id'],))
            user_resp = cur.fetchone()
        if user_resp:
            user_id = user_resp.id
            sql_query += " user_id = %s,"
            values.append(user_id)
        else:
            print "***\nuser not found!\n***"
            return

    if alarm['project_id']:
        project_id_q = ('SELECT id from projects'
                        ' WHERE uuid = %s')
        with PoolConnection() as cur:
            cur.execute(project_id_q, (alarm['project_id'],))
            project_resp = cur.fetchone()
        if project_resp:
            project_id = project_resp[0]
            sql_query += " project_id = %s,"
            values.append(project_id)
        else:
            print "***\nproject not found!\n***"
            return

    if alarm['enabled']:
        sql_query += ' enabled = %s,'
        values.append(alarm['enabled'])
    if alarm['name']:
        sql_query += ' name = %s,'
        values.append(alarm['name'])
    if alarm['description']:
        sql_query += ' description = %s,'
        values.append(alarm['description'])
    if alarm['state']:
        sql_query += ' state = %s,'
        values.append(alarm['state'])
    if alarm['alarm_actions']:
        sql_query += ' alarm_actions = %s,'
        values.append(Json(alarm['alarm_actions']))
    if alarm['ok_actions']:
        sql_query += ' ok_actions = %s,'
        values.append(Json(alarm['ok_actions']))
    if alarm['insufficient_data_actions']:
        sql_query += ' insufficient_data_actions = %s,'
        values.append(Json(alarm['insufficient_data_actions']))
    if alarm['repeat_actions']:
        sql_query += ' repeat_actions = %s,'
        values.append(alarm['repeat_actions'])
    if alarm['time_constraints']:
        sql_query += ' time_constraints = %s,'
        values.append(Json(alarm['time_constraints']))
    if alarm['rule']:
        sql_query += ' rule = %s'
        values.append(Json(alarm['rule']))

    sql_query = sql_query.rstrip(',')
    sql_query += ' WHERE alarm_id = %s'
    values.append(alarm['alarm_id'])

    with PoolConnection() as cur:
        print str(sql_query) + '\n\n'
        print str(values) + '\n\n'
        cur.execute(sql_query, values)
    stored_alarm = get_alarms(alarm_id=alarm['alarm_id'])
    return stored_alarm

user_id_1 = "0c76126d-a5e3-4503-be44-4d9bed645cf7"
user_id_2 = "3d622ea5-a70a-42d3-aae5-49ddfc1ef355"

project_id_1 = "128aef99-3efc-4f15-a93a-1d8e6daed4f0"
project_id_2 = "f2cc0bbd-0b72-4e41-b0b7-0059ea2b9f91"


updated_alarm = {
    'alarm_actions': [
        u'http: //site: 8080/'
    ],
    'ok_actions': [
        u'http: //site: 8080/'
    ],
    'description': u'TEST_DESCRIPTION',
    'state': 'alarm',
    'timestamp': datetime.datetime(2015, 4, 13, 10, 2, 24, 698967),
    'enabled': True,
    'state_timestamp': datetime.datetime(2015, 4, 13, 10, 2, 24, 698967),
    'rule': {
        'meter_name': u'cpu_util',
        'evaluation_periods': 3,
        'period': 888,
        'statistic': 'avg',
        'threshold': 999.0,
        'query': [
            {
                'field': u'resource_id',
                'type': u'',
                'value': u'465e284e-7351-4130-b668-bfa7980969f4',
                'op': 'eq'
            }
        ],
        'comparison_operator': 'ge',
        'exclude_outliers': False
    },
    'alarm_id': u'81ac9221-e5e3-4a56-bbc6-7d533e271a58',
    'time_constraints': [
        {
            'duration': 300,
            'start': '010***',
            'timezone': u'',
            'name': u'TC_NAME',
            'description': u'tesr'
        },
        {
            'duration': 300,
            'start': '010***',
            'timezone': u'',
            'name': u'TC_NAME_TEST',
            'description': u'Timeconstraintat010***lastingfor300seconds'
        },
        {
            'duration': 777,
            'start': '0102010*',
            'timezone': u'',
            'name': u'TEST_TC',
            'description': u'TEST_TC_DESCRIPTION'
        }
    ],
    'insufficient_data_actions': [
        u'http: //site: 8080/'
    ],
    'repeat_actions': False,
    'user_id': u'3d622ea5a70a42d3aae549ddfc1ef355',
    'project_id': u'',
    'type': 'threshold',
    'name': u'TEST_ALARM'
}

for key, value in updated_alarm.iteritems():
    print key

print update_alarm(updated_alarm)
