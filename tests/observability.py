"""Integration assertions for history availability, pagination, and profile identity."""
import concurrent.futures
import json
import time
import uuid
from urllib.parse import urlencode

from dbt.adapters.duckfleet.client import Client, RemoteConnection, RemoteError


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def checks(url, token):
    client = Client(url, token)
    run = str(uuid.uuid4())
    context = {'model_run_id': run, 'dbt': {'node_id': 'model.observability.fixture', 'invocation_id': run}}
    connection = RemoteConnection(url, token, context=context)
    ids = []

    def submit(statement, expect_failure=False):
        query_id = str(uuid.uuid4())
        ids.append(query_id)
        try:
            client.request('POST', connection.path+'/queries', {'request_id':query_id, 'sql':statement})
            require(not expect_failure, 'expected SQL error')
        except RemoteError as error:
            require(expect_failure and error.status == 400, str(error))
        return json.loads(client.request('GET', '/v1/queries/'+query_id))

    def page(**filters):
        return json.loads(client.request('GET', '/v1/queries?'+urlencode(filters)))

    try:
        first = submit('select sum(i) as observed_total from range(10000) t(i)')
        require(first['status'] == 'succeeded', str(first))
        require(first['profile_status'] == 'captured', str(first))
        require(first['profile']['query_name'] == first['sql'], 'profile belongs to different SQL')
        require(first['metrics']['rows_returned'] == 1, str(first['metrics']))
        require(first['metrics']['cpu_time'] >= 0, str(first['metrics']))
        failed = submit('select * from table_that_does_not_exist_for_profiling', True)
        require(failed['status'] == 'failed', str(failed))
        require((failed['profile'] or {}).get('query_name') != first['sql'], 'failure reused stale profile')
        submit('select 73 as next_profile')
        submit('select 74 as final_profile')
        found = []
        cursor = None
        while True:
            filters = {'limit':2, 'model_run_id':run}
            if cursor is not None:
                filters['before'] = cursor
            result = page(**filters)
            require(all('profile' not in q for q in result['queries']), 'list includes expensive raw plans')
            found.extend(q['query_id'] for q in result['queries'])
            cursor = result['next_before']
            if cursor is None:
                break
        require(found == list(reversed(ids)), f'pagination missed or duplicated records: {found}')
        filtered = page(model_id='model.observability.fixture', invocation_id=run, worker_id='small', status='failed')
        require([q['query_id'] for q in filtered['queries']] == [failed['query_id']], 'combined filters are wrong')
        require(page(model_id="' OR true --")['queries'] == [], 'filter input became SQL')
        for path, expected in [('/v1/queries?limit=501',400), ('/v1/queries?before=0',400),
                               ('/v1/queries?status=bogus',400), ('/v1/queries/'+str(uuid.uuid4()),404)]:
            try:
                client.request('GET', path)
            except RemoteError as error:
                require(error.status == expected, str(error))
            else:
                raise AssertionError(f'{path} should fail')
        for path in ['/v1/queries', '/v1/queries/'+first['query_id']]:
            try:
                Client(url, 'wrong').request('GET', path)
            except RemoteError as error:
                require(error.status == 401, str(error))
            else:
                raise AssertionError('history bypassed authentication')

        # Poll the durable running record while the same small worker is occupied.
        query_id = str(uuid.uuid4())
        long_sql = 'select sum(a.i*b.i) from range(1000000000) a(i),range(1000000000) b(i)'
        with concurrent.futures.ThreadPoolExecutor() as pool:
            future = pool.submit(client.request, 'POST', connection.path+'/queries',
                                 {'request_id':query_id,'sql':long_sql,'timeout_seconds':30})
            try:
                observed = False
                for _ in range(40):
                    started = time.monotonic()
                    running = page(model_run_id=run, status='running')
                    require(time.monotonic()-started < 3, 'history blocked behind DuckDB execution')
                    if any(q['query_id'] == query_id for q in running['queries']):
                        observed = True
                        break
                    time.sleep(0.05)
                require(observed, 'running query not visible independently of worker')
                started = time.monotonic()
                detail = json.loads(client.request('GET', '/v1/queries/'+query_id))
                require(time.monotonic()-started < 3 and detail['status'] == 'running', 'running detail blocked')
            finally:
                connection.cancel()
            try:
                future.result(timeout=5)
            except RemoteError as error:
                require(error.status == 400, str(error))
            else:
                raise AssertionError('long query did not cancel')
        detail = json.loads(client.request('GET', '/v1/queries/'+query_id))
        require(detail['status'] == 'canceled', 'cancellation missing from independent history')
        print('PASS: independent history, active queries, cursor pagination, filters, auth, and profile identity')
        return first['query_id']
    finally:
        connection.close()


def history_without_workers(url, token):
    client = Client(url, token)
    records = json.loads(client.request('GET', '/v1/queries?limit=10'))['queries']
    require(records, 'history disappeared while workers were stopped')
    detail = json.loads(client.request('GET', '/v1/queries/'+records[0]['query_id']))
    require(detail['query_id'] == records[0]['query_id'], 'query detail unavailable without workers')
    print('PASS: query history and details remain available with both workers stopped')


def save_model_profile(client, history, artifacts):
    invocation = json.loads((artifacts / 'resize-medium/target/run_results.json').read_text())['metadata']['invocation_id']
    candidates = [q for q in history if
        q['context'].get('dbt', {}).get('node_id') == 'model.duckfleet_harness.fct_orders'
        and q['context']['dbt']['invocation_id'] == invocation
        and ' '.join(q['sql'].lower().split()).startswith('create table')
        and 'fct_orders' in q['sql']
        and q['status'] == 'succeeded' and q['profile_status'] == 'captured']
    require(candidates, 'current dbt materialization has no execution profile')
    profile = json.loads(client.request('GET', '/v1/queries/'+candidates[0]['query_id']))
    require(profile['profile'].get('children') and profile['metrics'], 'dbt operator tree or metrics missing')
    require(profile['context']['model_run_id'], 'dbt profile lost model attribution')
    (artifacts / 'query-profile.json').write_text(json.dumps(profile, indent=2))
    print('PASS: current dbt table materialization has a durable operator profile and model identity')
