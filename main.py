#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# copyright (c) 2015 luffae@gmail.com
#

from flask import Flask, Response, request, json

app = Flask(__name__)

app.config.from_object('main')
app.config.from_envvar('FLASK_CONFIG')

from elasticsearch import Elasticsearch

es = Elasticsearch(app.config['ELASTICSEARCH'])


def do_query(field, keyword, start, end, host):
  query = {
    'filtered': {
      'query': {
        'match_phrase': {
          field: { 'query': keyword, 'operator': 'and' }
        }
      },
      'filter': {
        'and': [
          { 'range': { '@timestamp': { 'from': start, 'to': end } } }
        ]
      }
    }
  }

  if host is not 'all':
    term = { 'term': { 'host': host } }
    query['filtered']['filter']['and'].append(term)

  result = es.search(
    body = {
      'size': app.config['QUERY_SIZE'],
      'query': query
    }
  )

  data = []
  for r in result['hits']['hits']:
    s = r['_source']
    data.append({
      'time' : s['timestamp'],
      'host' : s['host'].split(':')[0],
      'info' : s[field]
    })

  return data


@app.route("/query", methods=["GET"])
def query():
  from datetime import datetime, timedelta
  from time import timezone

  td = timedelta(seconds=timezone)

  args = request.args
  host = args.get('host', 'all')
  rcnt = args.get('rcnt', 'n')

  logs = do_query(
         args['field'],
         args['keyword'],
         datetime.strptime(args['start'], '%Y%m%d%H%M') + td,
         datetime.strptime(args['end'], '%Y%m%d%H%M') + td,
         host)

  if rcnt == 'y':
    r = str(len(logs))
    m = "text/html"
  else:
    r = json.dumps(logs)
    m = "application/json"

  return Response(response=r, status=200, mimetype=m)


if __name__ == "__main__":
  app.run(host=app.config['SERVER_ADDR'], threaded=True)

