# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

**사용자에게 하는 대답은 항상 한글로 한다.**

## 개요

게임 랭킹을 **Redis Sorted Set**과 **MySQL(InnoDB)** 두 저장소로 구현해 속도를 비교하는 .NET 10 프로젝트. Kafka → Redis 멱등 반영 데모도 들어 있다. README는 핵심 결론만 담는다. 상세 수치와 해석은 `docs/측정결과.md`(브라우저용 `docs/측정결과.html`도 같이 고친다), 벤치마크 원본 출력은 `results/*.md`에 있다. 코드 주석, 문서, 로그 출력은 한글로 쓴다.

- GitHub: https://github.com/RockQoo/ranking-sever.git

## 명령어

테스트 프로젝트는 없다. 정합성 확인은 벤치마크 안에서 한다: Redis와 MySQL 결과가 같은지 비교하고, 다르면 예외를 던진다.

```bash
dotnet build -c Release                     # 솔루션 전체 (RankingServer.slnx)
wsl -d Ubuntu -- bash scripts/apply-sql.sh  # sql/ 스키마·SP를 DB에 적용 (고쳤을 때)

# 벤치마크 (결과는 results/<yyyyMMdd-HHmm>-users<N>[-접미사].md 로 저장)
dotnet run -c Release --no-build --project src/Ranking.Bench -- --users 100000
dotnet run -c Release --no-build --project src/Ranking.Bench -- --users 1000000 --window-ops 10
dotnet run -c Release --no-build --project src/Ranking.Bench -- --deep-page --users 1000000
dotnet run -c Release --no-build --project src/Ranking.Bench -- --snapshot --users 1000000
#   그 밖의 옵션: --ops --warmup --concurrency --throughput-ops --page-ops --snapshot-ops --redis --mysql

# API 서버 (Ranking:Backend = redis | mysql, 기본 redis)
dotnet run --project src/Ranking.Api -- --Ranking:Backend=mysql

# Kafka 데모
dotnet run -c Release --project src/Ranking.Kafka -- --users 10000 --events 200000
```

## 실행 환경

새 PC 셋팅을 요청받으면 [SETUP.md](SETUP.md) 순서대로 하고, 단계마다 확인 명령으로 이미 된 것은 건너뛴다. WSL 설치처럼 관리자 권한이나 재부팅이 필요한 단계는 사용자에게 맡긴다. WSL 쪽 설치는 `wsl -d Ubuntu -u root -- bash scripts/setup-wsl.sh --with-kafka`이고, 여러 번 실행해도 된다.

- Redis, MySQL, Kafka는 같은 PC의 **WSL2 Ubuntu**에서 systemd 서비스(`redis-server`, `mysql`, `kafka`)로 돈다. .NET 클라이언트는 Windows에서 `127.0.0.1`로 붙는다. MySQL DB `ranking`, 계정 `bench`/`bench`. Kafka는 `/opt/kafka`, `127.0.0.1:9092`.
- 데이터를 직접 보려면: `wsl -d Ubuntu -- redis-cli ...`, `wsl -d Ubuntu -- mysql -ubench -pbench ranking -e "..."`.
- WSL은 쉬면 몇 초 뒤 꺼지고 서비스도 같이 멈춘다. 측정 중에는 다른 창에서 `wsl -d Ubuntu -- sleep 3600`으로 붙잡아 둔다.
- Kafka는 `advertised.listeners=PLAINTEXT://127.0.0.1:9092`여야 한다(`localhost`로 광고하면 `::1`로 붙으려다 실패). `KAFKA_OPTS=-Djava.net.preferIPv4Stack=true`도 필요하다.
- 벤치마크와 데모는 **데이터를 지운다**: `ResetAsync`가 `ranking`(스냅숏·깊은 페이지 모드면 `ranking_snapshot`, `users`도)을 TRUNCATE 하고 `ranking:season` ZSET을 삭제한다. Kafka 데모는 토픽과 Redis 키를 새로 만든다.

## MySQL 스키마와 저장 프로시저

- 테이블·인덱스는 `sql/schema.sql`, 게임 서버가 쓰는 쿼리는 전부 `sql/procedures.sql`의 SP다. C#은 `CALL sp_xxx(@a, …)`만 한다(`MySqlProc` 도우미). 테이블을 코드에서 만들지 않는다.
- SP나 스키마를 고치면 `wsl -d Ubuntu -- bash scripts/apply-sql.sh`로 DB에 다시 적용해야 C# 쪽에 반영된다. 측정이 돌고 있을 때 적용하면 SP를 DROP/CREATE 하는 사이에 호출이 실패한다. `schema.sql`은 `IF NOT EXISTS`라 기존 테이블 구조는 바꾸지 않는다.
- 예외로 코드에 남긴 SQL: 벤치마크용 대량 적재(다중 행 INSERT, `TRUNCATE users`, `ANALYZE`)와 `EXPLAIN`용 SELECT(`CALL`에는 `EXPLAIN`을 못 써서 SP 본문을 복사해 둠 — `DeepPageBench`, `SnapshotBench`. SP를 고치면 같이 고친다).

## 구조

- `Ranking.Core` — `IRankingStore` 인터페이스와 구현 3개(`RedisRankingStore`, `MySqlRankingStore`, `MySqlSnapshotRankingStore`). API와 벤치마크가 같이 쓴다.
  - **두 구현이 반드시 지켜야 하는 정렬 기준:** 점수 내림차순, 동점이면 `user_id` 내림차순. Redis는 동점일 때 member 사전순이라 member를 `userId.ToString("D10")`(고정 폭)으로 저장해 사전순 = 숫자순이 되게 했다. 저장소나 쿼리를 새로 만들 때도 이 기준을 지켜야 벤치마크의 교차 검증이 통과한다.
  - MySQL 스키마: `ranking(user_id PK, score, KEY ix_score(score, user_id))`. 순위는 `1 + COUNT(score > s) + COUNT(score = s AND user_id > u)`로 구한다. `(score, user_id) > (…)` 같은 행 생성자 비교는 MySQL이 인덱스를 잘 못 써서 일부러 피했다. `MySqlRankMode.Window`(`ROW_NUMBER()`)는 비교용 기준선이다.
  - `MySqlSnapshotRankingStore`는 쓰기는 원본 `ranking`, 읽기는 배치로 미리 계산한 `ranking_snapshot(user_id PK, score, rnk UNIQUE)`. `RebuildAsync`로 재계산하며 방식은 바꿔치기(새 테이블 → `ANALYZE` → 원자적 `RENAME`) / TRUNCATE / 한 트랜잭션 DELETE 세 가지. 재계산 세션은 기본으로 READ COMMITTED다(REPEATABLE READ + binlog면 `INSERT … SELECT`가 원본 행을 잠가 점수 갱신이 막히고 데드락이 난다). `ANALYZE`를 빼면 새 테이블 통계가 비어 있어 `rnk` 인덱스를 안 쓴다.
  - 순위는 1부터(`RankEntry.Rank`), `GetPageAsync`의 offset은 0부터.
- `Ranking.Bench` — 시드 고정 난수로 두 저장소에 같은 데이터를 넣고, 결과가 같은지 확인한 뒤 순차 지연 시간과 동시 처리량을 잰다. `DeepPageBench`(`--deep-page`)는 닉네임(`users` JOIN)을 포함한 랭킹 목록을 offset별로 단순 JOIN / 지연 조인 / 순위 테이블 / Redis(+닉네임 해시 `ranking:nick`)로 비교하고 `EXPLAIN` 결과도 남긴다. `SnapshotBench`(`--snapshot`)는 순위 테이블 조회 속도, 재계산 방식별로 재계산 중 조회 실패/지연, 격리 수준별로 재계산 중 점수 갱신 지연을 잰다.
- `Ranking.Api` — ASP.NET Core Minimal API. `Ranking:*` 설정(`appsettings.json`)으로 `IRankingStore` 하나를 싱글턴으로 고른다. 엔드포인트: `PUT /scores/{userId}`, `GET /ranks/{userId}`, `GET /top?n=`, `GET /around/{userId}?range=`.
- `Ranking.Kafka` — `Program.cs` 하나로 된 데모. 프로듀서(멱등, `acks=all`, 키 = user_id라 같은 파티션 안에서 유저별 순서 보장) → 자동 커밋을 끈 컨슈머가 Redis에 **반영한 뒤** 커밋(at-least-once). 중복은 Lua 스크립트가 유저별 `seq`가 저장된 마지막 seq보다 클 때만 `ZINCRBY`해서 걸러낸다. 데모는 컨슈머 A를 반영과 커밋 사이에 일부러 죽이고, 멱등 반영과 그냥 반영한 결과를 비교한다.
