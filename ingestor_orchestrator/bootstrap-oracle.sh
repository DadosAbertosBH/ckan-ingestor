#!/usr/bin/env bash
# Copyright (C) 2026 Noctcloud Desenvolvimento LTDA
# SPDX-License-Identifier: AGPL-3.0-or-later
set -euo pipefail

case "$ORACLE_PWD$APP_PASSWORD" in
  *\"*|*\'*|*$'\n'*|*$'\r'*) echo 'Oracle passwords must exclude quotes and newlines' >&2; exit 1 ;;
esac

bootstrap() {
sqlplus -L -S /nolog <<SQL
WHENEVER SQLERROR EXIT SQL.SQLCODE
CONNECT system/"$ORACLE_PWD"@"(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=oracle)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=FREEPDB1)))"

DECLARE
  n NUMBER;
BEGIN
  SELECT COUNT(*) INTO n FROM dba_tablespaces WHERE tablespace_name='CKAN_DATA';
  IF n=0 THEN
    EXECUTE IMMEDIATE 'CREATE TABLESPACE CKAN_DATA DATAFILE ''/opt/oracle/oradata/FREE/FREEPDB1/ckan_data01.dbf'' SIZE 256M AUTOEXTEND ON NEXT 64M MAXSIZE UNLIMITED';
  END IF;
END;
/

DECLARE
  n NUMBER;
BEGIN
  SELECT COUNT(*) INTO n FROM all_users WHERE username='CKAN_APP';
  IF n=0 THEN EXECUTE IMMEDIATE 'CREATE USER CKAN_APP IDENTIFIED BY "$APP_PASSWORD"'; END IF;
END;
/
GRANT CONNECT, RESOURCE TO CKAN_APP;
ALTER USER CKAN_APP DEFAULT TABLESPACE CKAN_DATA QUOTA UNLIMITED ON CKAN_DATA;

EXIT
SQL
}

for attempt in {1..60}; do
  if bootstrap; then
    exit 0
  fi
  sleep 2
done

echo 'Oracle FREEPDB1 did not become ready for bootstrap' >&2
exit 1
