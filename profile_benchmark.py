#!/usr/bin/env python
from __future__ import print_function
from time import time
import sys
import sqlite3
import json
from uuid import uuid4
conn = sqlite3.connect('places.backup.sqlite', detect_types=sqlite3.PARSE_COLNAMES)
cur = conn.cursor()

LIMITS = (10, 100, 1000)

queries = {
    'mak_1': (
        'SELECT'
        '(SELECT url FROM moz_places WHERE rev_host = h.rev_host ORDER BY frecency DESC, last_visit_date DESC LIMIT 1) AS url,'
        '(SELECT title FROM moz_places WHERE rev_host = h.rev_host ORDER BY frecency DESC, last_visit_date DESC LIMIT 1) AS title,'
        '(SELECT frecency FROM moz_places WHERE rev_host = h.rev_host ORDER BY frecency DESC, last_visit_date DESC LIMIT 1) AS frecency,'
        '(SELECT last_visit_date FROM moz_places WHERE rev_host = h.rev_host ORDER BY frecency DESC, last_visit_date DESC LIMIT 1) AS lastVisitDate,'
        '"history" as type '
        'FROM moz_places h '
        'WHERE rev_host IN ('
        '    SELECT DISTINCT rev_host FROM moz_places'
        '    WHERE hidden = 0 AND last_visit_date NOTNULL'
        '    ORDER by frecency DESC, last_visit_date DESC'
        '    LIMIT ?'
        ') '
        'GROUP BY rev_host '
        'ORDER BY frecency DESC, lastVisitDate DESC, url '
        'LIMIT 10'),

    'mak_2': (
        'SELECT url, title, frecency, last_visit_date as lastVisitDate, "history" as type '
        'FROM moz_places '
        'WHERE frecency IN ('
        '    SELECT MAX(frecency) AS frecency FROM moz_places '
        '    WHERE rev_host IN ('
        '        SELECT DISTINCT rev_host FROM moz_places'
        '        WHERE hidden = 0 AND last_visit_date NOTNULL'
        '        ORDER by frecency DESC, last_visit_date DESC'
        '        LIMIT ?'
        '    ) '
        '    GROUP BY rev_host'
        ') '
        'GROUP BY rev_host HAVING MAX(last_visit_date) '
        'ORDER BY frecency DESC, last_visit_date DESC'),

    'maksik_2': (
        'SELECT url, title, frecency, last_visit_date as lastVisitDate, "history" as type '
        'FROM moz_places '
        'WHERE frecency in ('
        '  SELECT MAX(frecency) as frecency'
        '  FROM moz_places'
        '  WHERE hidden = 0 AND last_visit_date NOTNULL'
        '  GROUP BY rev_host'
        '  ORDER BY frecency DESC'
        '  LIMIT ?'
        ') '
        'GROUP BY rev_host HAVING MAX(lastVisitDate) '
        'ORDER BY frecency DESC, lastVisitDate DESC')
}

sorted_keys = sorted(queries.keys())
query_results = {limit:{key:[] for key in sorted_keys} for limit in LIMITS}

for key in sorted_keys:
    for limit in LIMITS:
        query_results[limit][key] = json.dumps(cur.execute(queries[key], [limit]).fetchall())

        benchmark = []
        for i in xrange(10):
            start = time()
            cur.execute(queries[key], [limit]).fetchall()
            end = time()
            benchmark.append(end-start)

        benchmark = sorted(benchmark)
        limit_str = str(limit).ljust(5)
        mean = str(int(sum(benchmark)/len(benchmark) * 1000)).rjust(4)
        median = str(int(benchmark[len(benchmark)/2] * 1000)).rjust(4)
        print("results for\t{}@{}\tmean: {}ms\tmedian: {}ms".format(key.rjust(10), limit, mean, median))

for limit in LIMITS:
    same = True
    prev = None
    for key in sorted_keys[1:]:
        if prev is None:
            prev = query_results[limit][key]
        else:
            same &= (prev == query_results[limit][key])
            if not same:
                break
    print("{}@{} same: {}".format(sorted_keys, str(limit).ljust(5), same))
