"""Exercise actual PostgreSQL role provisioning in a disposable Compose project.

Only PostgreSQL/provisioning run; no application builds or existing project volumes
are touched. Connect through service DNS so SCRAM is tested, not local trust auth.
"""
import json
import os
from pathlib import Path
import subprocess
import uuid

ROOT = Path(__file__).resolve().parents[1]


def main():
    project = 'duckfleet-provision-test-' + uuid.uuid4().hex[:12]
    base = ['docker', 'compose', '-f', str(ROOT/'compose.yaml'), '-p', project]
    # A developer's existing-volume admin override must not influence this fresh
    # fixture or make the test inherit a real password unnecessarily.
    environment = dict(os.environ)
    environment['DUCKFLEET_POSTGRES_ADMIN_PASSWORD'] = 'local-administrator-password'
    environment['DUCKFLEET_ADOPT_DATABASE_OWNER'] = 'no'
    resolved = subprocess.run(base+['config','--format','json'], cwd=ROOT, env=environment,
                              text=True, capture_output=True, check=True, timeout=60)
    settings = json.loads(resolved.stdout)['services']['platform-provision']['environment']
    passwords = [settings['DUCKFLEET_DB_'+role+'_PASSWORD'] for role in
                 ('MIGRATOR','GATEWAY','WORKER','MAINTAINER','CATALOG')]

    def redacted(text):
        for password in passwords:
            text = text.replace(password, '[redacted]')
        return text

    def command(arguments, succeeds=True):
        result = subprocess.run(base+arguments, cwd=ROOT, env=environment,
                                text=True, capture_output=True, timeout=180)
        if succeeds and result.returncode:
            raise RuntimeError('database fixture command failed: '+redacted(result.stderr[-1200:]))
        return result

    def query(role, database, sql, allowed=True, password=None):
        password = password or settings['DUCKFLEET_DB_'+role.upper()+'_PASSWORD']
        name = settings['DUCKFLEET_DB_'+role.upper()+'_ROLE']
        result = command(['exec','-T','-e','PGPASSWORD='+password,'postgres','psql','-X',
                          '-h','postgres','-U',name,'-d',database,'-v','ON_ERROR_STOP=1','-tAc',sql],False)
        if (result.returncode == 0) != allowed:
            raise RuntimeError(f'{role}/{database} expected allowed={allowed}: '+redacted(result.stderr[-500:]))
        return result.stdout.strip()

    def admin(sql, database='postgres'):
        return command(['exec','-T','postgres','psql','-X','-U','duckfleet','-d',database,
                        '-v','ON_ERROR_STOP=1','-tAc',sql]).stdout.strip()

    try:
        command(['up','-d','--wait','--wait-timeout','60','postgres'])
        command(['run','--rm','--no-deps','platform-provision'])
        identities = "SELECT rolname,oid FROM pg_roles WHERE rolname LIKE 'duckfleet_%' ORDER BY rolname"
        before = admin(identities)
        command(['run','--rm','--no-deps','platform-provision'])
        assert before == admin(identities), 'repeat provisioning changed role identities'
        print('PASS: fresh/repeated provisioning preserves five separate role identities', flush=True)
        for role in ('migrator','gateway','worker','maintainer','catalog'):
            own = 'lake' if role == 'catalog' else 'platform'
            other = 'platform' if role == 'catalog' else 'lake'
            assert query(role,own,"SELECT current_user=session_user AND NOT (rolsuper OR rolcreatedb OR rolcreaterole OR rolreplication OR rolbypassrls) AND NOT EXISTS(SELECT 1 FROM pg_auth_members WHERE member=pg_roles.oid) FROM pg_roles WHERE rolname=current_user") == 't'
            query(role,other,'SELECT 1',False)
            query(role,own,'CREATE ROLE forbidden_role',False)
        for role in ('gateway','worker','maintainer'):
            query(role,'platform','CREATE TABLE public.forbidden_table(id int)',False)
            query(role,'platform','SET ROLE duckfleet_migrator',False)
        query('migrator','platform','SELECT 1',False,settings['DUCKFLEET_DB_WORKER_PASSWORD'])
        print('PASS: real network logins reject cross-database access, DDL, role escalation, and password reuse', flush=True)
        assert admin("SELECT pg_get_userbyid(datdba) FROM pg_database WHERE datname='platform'") == 'duckfleet_migrator'
        assert admin("SELECT pg_get_userbyid(datdba) FROM pg_database WHERE datname='lake'") == 'duckfleet_catalog'
        admin('ALTER DATABASE platform OWNER TO duckfleet')
        admin('CREATE TABLE public.legacy_owner_fixture(id int)','platform')
        refused = command(['run','--rm','--no-deps','platform-provision'],False)
        assert refused.returncode != 0 and 'owner differs' in refused.stdout+refused.stderr, 'owner refusal must return a failing exit status'
        owner = "SELECT pg_get_userbyid(relowner) FROM pg_class WHERE oid='legacy_owner_fixture'::regclass"
        assert admin(owner,'platform') == 'duckfleet'
        command(['run','--rm','--no-deps','-e','DUCKFLEET_ADOPT_DATABASE_OWNER=yes','platform-provision'])
        assert admin(owner,'platform') == 'duckfleet_migrator'
        assert admin("SELECT pg_get_userbyid(datdba) FROM pg_database WHERE datname='lake'") == 'duckfleet_catalog'
        print('PASS: legacy-owner adoption is explicit and does not reassign the other database', flush=True)
    finally:
        command(['down','--volumes','--remove-orphans'])
        print('PASS: disposable role-provisioning project and volumes removed', flush=True)


if __name__ == '__main__':
    main()
