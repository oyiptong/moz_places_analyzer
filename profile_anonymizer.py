#!/usr/bin/env python
from __future__ import print_function
import sys
import sqlite3
from uuid import uuid4
conn = sqlite3.connect('places.backup.sqlite', detect_types=sqlite3.PARSE_COLNAMES)
conn.row_factory = sqlite3.Row
cur = conn.cursor()

drop_tables = [
    'moz_historyvisits',
    'moz_inputhistory',
    'moz_bookmarks',
    'moz_bookmarks_roots',
    'moz_keywords',
    'moz_favicons',
    'moz_annos',
    'moz_anno_attributes',
    'moz_items_annos',
    'moz_hosts']

# remove extraneous tables
for t in drop_tables:
    try:
        cur.execute('DROP TABLE {}'.format(t))
        conn.commit()
        print('dropped:\t{}'.format(t))
    except Exception as e:
        print('warning:\t{}'.format(e.message))

host_map = {}
rows = cur.execute('SELECT DISTINCT(rev_host) FROM moz_places').fetchall()
for row in rows:
    rev_host = row['rev_host']
    host_map[rev_host] = str(uuid4())

total_count = cur.execute('SELECT COUNT(*) as total FROM moz_places').fetchone()['total']
count = 0

for rev_host, uid in host_map.iteritems():
    rows = cur.execute('SELECT id, guid FROM moz_places WHERE rev_host = ? ORDER BY id', [rev_host]).fetchall()
    for row in rows:
        cur.execute('UPDATE moz_places SET rev_host = ?, url = ?, title = ? WHERE id = ?', [host_map[rev_host], row['guid'], row['guid'], row['id']])
        count += 1
        print('anonymizing:\t{: 9d}/{} records\r'.format(count, total_count), end='')

rows = cur.execute('SELECT id, guid FROM moz_places WHERE rev_host is NULL').fetchall()
for row in rows:
    cur.execute('UPDATE moz_places SET url = ? WHERE id = ?', [row['guid'], row['id']])
    count += 1
    print('anonymizing:\t{: 9d}/{} records\r'.format(count, total_count), end='')

print('\ncommitting')
conn.commit()
conn.close()
