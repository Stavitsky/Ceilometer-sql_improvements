#  install first_last_agg from PGXN
#  FAQ: http://pgxn.org/faq/


def get_resources(self, user=None, project=None, source=None,
                  start_timestamp=None, start_timestamp_op=None,
                  end_timestamp=None, end_timestamp_op=None,
                  metaquery={}, resource=None, pagination=None):
    if pagination:
        raise ceilometer.NotImplementedError('Pagination not implemented')

    s_filter = storage.SampleFilter(user=user,
                                    project=project,
                                    source=source,
                                    start_timestamp=start_timestamp,
                                    start_timestamp_op=start_timestamp_op,
                                    end_timestamp=end_timestamp,
                                    end_timestamp_op=end_timestamp_op,
                                    metaquery=metaquery,
                                    resource=resource)

    resource_id = None
    user_id = None
    project_id = None
    source_id = None

    subq_values = []
    samples_subq = ("SELECT resource_id, source_id, user_id, project_id,"
                    " max(timestamp) as max_ts, min(timestamp) as min_ts,"
                    " LAST(metadata) as metadata"
                    " FROM samples")

    if s_filter.resource:
        resource_id_q = ("SELECT id FROM resources"
                         " WHERE resource_id = %s")
        with PoolConnection(self.conn_pool) as cur:
            cur.execute(resource_id_q, (s_filter.resource,))
            resource_resp = cur.fetchone()
        if resource_resp:
            resource_id = resource_resp[0]
            samples_subq += " AND resource_id = %s"
            subq_values.append(resource_id)
        else:
            LOG.debug(_("Resource from sample filter does not exist in DB"))
            return tuple()

    if s_filter.user:
        user_id_q = ("SELECT id FROM users"
                     " WHERE uuid = %s")
        with PoolConnection(self.conn_pool) as cur:
            cur.execute(user_id_q, (s_filter.user,))
            user_resp = cur.fetchone()
        if user_resp:
            user_id = user_resp[0]
            samples_subq += " AND user_id = %s"
            subq_values.append(user_id)
        else:
            LOG.debug(_("User from sample filter does not exist in DB"))
            return tuple()

    if s_filter.project:
        project_id_q = ("SELECT id FROM projects"
                        " WHERE uuid = %s")
        with PoolConnection(self.conn_pool) as cur:
            cur.execute(project_id_q, (s_filter.project,))
            project_resp = cur.fetchone()
        if project_resp:
            project_id = project_resp[0]
            samples_subq += " AND project_id = %s"
            subq_values.append(project_id)
        else:
            LOG.debug(_("Project from sample filter does not exist in DB"))
            return tuple()

    if s_filter.source:
        source_name_q = ("SELECT id FROM sources"
                         " WHERE name = %s")
        with PoolConnection(self.conn_pool) as cur:
            cur.execute(source_name_q, (s_filter.source,))
            source_resp = cur.fetchone()
        if source_resp:
            source_id = source_resp[0]
            samples_subq += " AND source_id = %s"
            subq_values.append(source_id)
        else:
            LOG.debug(_("Source from sample filter does not exist in DB"))
            return tuple()

    if s_filter.start:
        ts_start = s_filter.start
        if s_filter.start_timestamp_op == "gt":
            samples_subq += " AND timestamp > %s"
        else:
            samples_subq += " AND timestamp >= %s"
        subq_values.append(ts_start)

    if s_filter.end:
        ts_end = s_filter.end
        if s_filter.end_timestamp_op == 'le':
            samples_subq += " AND timestamp <= %s"
        else:
            samples_subq += " AND timestamp < %s"
        subq_values.append(ts_end)

    if s_filter.metaquery:
        q, v = apply_metaquery_filter(s_filter.metaquery)
        samples_subq += " AND {}".format(q)
        subq_values.append(v)

    samples_subq += (" GROUP BY resource_id, source_id,"
                     " user_id, project_id"
                     " ORDER BY resource_id ASC")

    samples_subq = samples_subq.replace(" AND", " WHERE", 1)

    query = ("SELECT resources.resource_id as id, sources.name as source_name,"
             " users.uuid as user_id, projects.uuid as project_id,"
             " max_ts, min_ts, metadata"
             " FROM ({}) as samples"
             " JOIN resources ON samples.resource_id = resources.id"
             " JOIN users ON samples.user_id = users.id"
             " JOIN projects ON samples.project_id = projects.id"
             " JOIN sources ON samples.source_id = sources.id")

    query = query.format(samples_subq)

    with PoolConnection(self.conn_pool) as cur:
        cur.execute(query, subq_values)
        resp = cur.fetchall()

    return (api_models.Resource(
        resource_id=res[0],
        user_id=res[2],
        project_id=res[3],
        first_sample_timestamp=res[4],
        last_sample_timestamp=res[5],
        source=res[1],
        metadata=res[6]) for res in resp)
