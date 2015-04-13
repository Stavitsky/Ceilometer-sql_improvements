import datetime


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
        with PoolConnection(self.conn_pool) as db:
            db.execute(user_id_q, (alarm['user_id'],))
            user_resp = db.fetchone()
        if user_resp:
            user_id = user_resp.id
            sql_query += " user_id = %s,"
            values.append(user_id)
        else:
            LOG.debug(_("User does not exist in DB"))
            return

    if alarm['project_id']:
        project_id_q = ('SELECT id from projects'
                        ' WHERE uuid = %s')
        with PoolConnection(self.conn_pool) as db:
            db.execute(project_id_q, (alarm['project_id'],))
            project_resp = db.fetchone()
        if project_resp:
            project_id = project_resp.id
            sql_query += " project_id = %s,"
            values.append(project_id)
        else:
            LOG.debug(_("Project does not exist in DB"))
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

    with PoolConnection(self.conn_pool, cursor_factory=DictCursor) as db:
        db.execute(sql_query, values)
        stored_alarm = get_alarms(alarm_id=alarm['alarm_id'])
    return stored_alarm
