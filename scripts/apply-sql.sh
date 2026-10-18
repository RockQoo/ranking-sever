#!/usr/bin/env bash
# sql/schema.sql(테이블·인덱스)과 sql/procedures.sql(저장 프로시저)을 DB ranking에 적용한다.
# 여러 번 실행해도 된다. SP를 고쳤으면 다시 실행한다.
#
# Windows에서: wsl -d Ubuntu -- bash scripts/apply-sql.sh
set -euo pipefail

cd "$(dirname "$0")/.."
export MYSQL_PWD=bench
mysql -ubench ranking < sql/schema.sql
mysql -ubench ranking < sql/procedures.sql
echo "적용 완료: $(mysql -ubench ranking -N -e "SELECT COUNT(*) FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA = 'ranking'")개 프로시저"
