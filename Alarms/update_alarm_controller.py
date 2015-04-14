def update_alarm(alarm):
    alarm = alarm.as_dict()
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

    # Note (alexstav): "'key' in dict" form required
    # for values with bool or empty array types
    if 'enabled' in alarm:
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
    if 'repeat_actions' in alarm:
        sql_query += ' repeat_actions = %s,'
        values.append(alarm['repeat_actions'])
    if 'time_constraints' in alarm:
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
    # returns first Alarm object from generator
    stored_alarm = get_alarms(alarm_id=alarm['alarm_id']).next()
    return stored_alarm
