#!/usr/bin/python
# -*- coding: utf-8 -*-

import psycopg2

con = None

try:
    con = psycopg2.connect("dbname=ceilometer_test user=alexstav")
    cur = con.cursor()

    cur.execute('CREATE TABLE IF NOT EXISTS prebill ('
                ' seq bigint PRIMARY KEY,'
                ' class text,'
                ' obj_key uuid,'
                ' event_type text,'  # should be event_type?
                ' stamp_start timestamp,'
                ' stamp_end timestamp,'
                ' volume float,'
                ' owner_key uuid,'
                ' tenant_key uuid,'
                ' counter_type text,'  # should be counter_type?
                ' last_value float,'
                ' last_timestamp timestamp)'
                )
    con.commit()

finally:
    if con:
        con.close()
