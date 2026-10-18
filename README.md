# ranking-server

C#(.NET 10)으로 만든 랭킹 서버와 벤치마크. 같은 기능을 **Redis Sorted Set**과 **MySQL(InnoDB)** 두 저장소로 구현하고 속도를 비교했다.

## 구성

| 경로 | 내용 |
|---|---|
| `src/Ranking.Core` | `IRankingStore` 인터페이스와 구현 (`RedisRankingStore`, `MySqlRankingStore`, 배치로 미리 계산한 순위 테이블 `MySqlSnapshotRankingStore`) |
| `src/Ranking.Bench` | 벤치마크. 두 저장소에 같은 데이터를 넣고 결과가 같은지 확인한 뒤 측정 |
| `src/Ranking.Api` | ASP.NET Core Minimal API 랭킹 서버. 저장소는 설정으로 고른다 |
| `src/Ranking.Kafka` | Kafka → Redis 랭킹 반영 데모. at-least-once 재전달과 멱등 반영 확인 |
| `results/` | 측정 결과 원본 (Markdown) |

정렬 기준은 두 구현이 같다: **점수 내림차순, 동점이면 user_id 내림차순**.
Redis ZSET은 동점일 때 member 사전순으로 정렬하므로 member를 `D10` 고정 폭 문자열로 저장해 숫자 순서와 맞췄다.

## 기능별 구현

| 기능 | Redis | MySQL |
|---|---|---|
| 점수 갱신 | `ZADD` — O(log N) | `INSERT … ON DUPLICATE KEY UPDATE` + `(score, user_id)` 인덱스 갱신 — O(log N) + 커밋 fsync |
| 내 순위 | `ZREVRANK` — O(log N) | 나보다 앞선 행을 `COUNT(*)` (인덱스 범위 스캔) — **O(내 순위)** |
| 내 순위 (비교용) | — | `ROW_NUMBER() OVER (ORDER BY …)` — 매번 전체 정렬, O(N log N) |
| 상위 N명 | `ZREVRANGE 0 N-1 WITHSCORES` — O(log N + N) | `ORDER BY score DESC, user_id DESC LIMIT N` — 인덱스 역방향 스캔 |
| 깊은 페이지 (offset부터 N명) | `ZREVRANGE offset offset+N-1` — O(log N + N) | `… LIMIT N OFFSET offset` — 앞의 offset개를 읽고 버림, **O(offset)** |
| 내 주변 ±k | `ZREVRANK` → `ZREVRANGE` (2회 왕복) | 순위 `COUNT` → 위·아래 `LIMIT k` UNION (2회 왕복) |

## 실행

처음 받았으면 [SETUP.md](SETUP.md)대로 셋팅한다(WSL에 Redis·MySQL·Kafka 설치는 `scripts/setup-wsl.sh` 하나로 끝난다).

```bash
dotnet build -c Release
dotnet run -c Release --no-build --project src/Ranking.Bench -- --users 100000
dotnet run -c Release --no-build --project src/Ranking.Bench -- --users 1000000 --window-ops 10
dotnet run -c Release --no-build --project src/Ranking.Bench -- --deep-page --users 1000000   # 깊은 페이지 조회만
dotnet run -c Release --no-build --project src/Ranking.Bench -- --snapshot --users 1000000    # 순위 테이블 배치 재계산 + 바꿔치기

# API (기본 redis)
dotnet run --project src/Ranking.Api -- --Ranking:Backend=mysql
#   PUT /scores/{userId}  {"score":123}
#   GET /ranks/{userId}   GET /top?n=100   GET /around/{userId}?range=5
```

> Kafka는 `/opt/kafka`, systemd 서비스 `kafka`. Windows에서 붙으려면 `KAFKA_OPTS=-Djava.net.preferIPv4Stack=true`(IPv4 바인딩)와 `advertised.listeners=PLAINTEXT://127.0.0.1:9092`가 필요했다. `localhost`로 광고하면 클라이언트가 `::1`로 붙으려다 실패한다.
>
> WSL은 쉬면 몇 초 뒤 꺼져서 서비스도 같이 멈춘다. 측정 중에는 다른 창에서 `wsl -d Ubuntu -- sleep 3600`으로 붙잡아 둔다.

## 측정 결과 (2026-10-16)

환경: 같은 PC. 클라이언트는 Windows 11의 .NET 10, 서버는 WSL2 Ubuntu 26.04의 Redis 8.0.5와 MySQL 8.4.11(둘 다 기본 설정: Redis는 AOF 꺼짐, MySQL은 `innodb_flush_log_at_trx_commit=1`, buffer pool 128MB). localhost 왕복이 약 0.2ms라 Redis 수치는 대부분 왕복 시간이다. 측정은 한 번씩만 했다.

### 순차 지연 시간 — 평균 ms (클라이언트 1개)

| 연산 | 10만 명 Redis | 10만 명 MySQL | 100만 명 Redis | 100만 명 MySQL |
|---|---:|---:|---:|---:|
| 내 순위 (무작위 유저) | 0.24 | 3.87 | 0.25 | **33.1** |
| 내 순위 (상위 1%) | 0.23 | 0.71 | 0.23 | 0.98 |
| 내 순위 (하위 1%) | 0.25 | 7.00 | 0.23 | **64.1** |
| 내 순위 (윈도 함수) | — | 49.1 | — | **1,120** |
| 상위 100명 | 0.27 | 0.66 | 0.28 | 0.65 |
| 내 주변 ±5명 | 0.50 | 5.18 | 0.49 | 36.3 |
| 점수 갱신 | 0.22 | 2.92 | 0.25 | 2.72 |

### 동시 처리량 — 초당 처리 (32개 작업 동시)

| 연산 | 10만 명 Redis | 10만 명 MySQL | 100만 명 Redis | 100만 명 MySQL |
|---|---:|---:|---:|---:|
| 점수 갱신 | 91,309 | 4,067 | 57,915 | 4,896 |
| 내 순위 (무작위 유저) | 80,702 | 2,267 | 71,203 | **223** |
| 상위 100명 | 48,820 | 16,416 | 27,542 | 14,428 |

### 영속화 수준을 맞춘 쓰기 비교 — 10만 명, 점수 갱신

| 설정 | 순차 평균 ms | 동시 초당 처리 |
|---|---:|---:|
| Redis 기본 (AOF 꺼짐, RDB 스냅숏만) | 0.22 | 91,309 |
| Redis AOF `appendfsync everysec` | 0.23 | 70,927 |
| Redis AOF `appendfsync always` | **1.52** | 12,171 |
| MySQL `innodb_flush_log_at_trx_commit=1` (기본) | 2.92 | 4,067 |
| MySQL `innodb_flush_log_at_trx_commit=2` | 1.94 | 6,784 |

Redis와 MySQL 결과(순위 203건, 상위 100명, 주변 ±5명 20건)는 적재 직후와 갱신 이후 모두 **불일치 0건**이다.

## 해석

1. **차이가 가장 큰 건 "내 순위"다.** Redis는 인원이 10배 늘어도 0.24ms 그대로다(O(log N)). MySQL `COUNT` 방식은 내 앞에 있는 사람 수만큼 인덱스를 읽어서, 상위 1%는 1ms 안쪽이지만 하위 1%는 100만 명에서 64ms다. `EXPLAIN`으로 보면 `ix_score` 범위 스캔(커버링 인덱스)이고 rows가 대략 내 순위만큼 나온다.
   - 이유: Redis ZSET의 skiplist는 각 링크에 건너뛰는 원소 수(span)를 저장해서 순위를 내려가면서 바로 더한다. InnoDB B+Tree는 노드에 하위 원소 수를 저장하지 않아서 순위를 알려면 세는 수밖에 없다.
2. **윈도 함수로 순위를 구하면 매번 전체 정렬이다.** 100만 명에서 1.1초. 요청마다 쓰면 안 되고, 정산 배치에서 한 번 계산해 테이블로 저장하는 용도다.
3. **상위 N명은 둘 다 빠르다.** MySQL도 `(score, user_id)` 인덱스를 역방향으로 100개만 읽으면 끝이라 인원과 무관하게 0.65ms다. 차이는 왕복과 처리 오버헤드 정도다.
4. **쓰기 차이의 큰 부분은 영속화 보장 수준이다.** Redis 기본 설정은 매 쓰기를 디스크에 남기지 않는다. `appendfsync always`로 맞추면 Redis도 1.5ms로 느려진다. MySQL은 커밋마다 redo log fsync를 하는데, `flush_log_at_trx_commit=2`로 바꾸면 2.9ms → 1.9ms가 된다.
5. **동시 요청에서 MySQL 순위 조회는 무너진다.** 100만 명에서 초당 223건, p99 400ms. 순위 조회 한 번이 수십만 개 인덱스 항목을 읽어서 CPU와 buffer pool을 오래 잡기 때문이다.

## 깊은 페이지 조회 (Deep Pagination) — 100만 명, 2026-10-17

"랭킹 더 보기"로 offset 위치부터 10명을 읽는 비용. 외부 자료(MySQL `ORDER BY score DESC LIMIT 10 OFFSET n` vs Redis `ZREVRANGE`, 100만 건, offset 90만에서 MySQL 690ms · Redis 1.9ms)와 같은 조건을 재현해 비교했다. MySQL은 두 스키마로 쟀다.

- **mysql**: 이 프로젝트 스키마. `(score, user_id)` 커버링 인덱스
- **mysql-score-only**: 외부 자료와 같은 형태. `(user_id, username, score)` + `KEY(score)`, `SELECT`에 username 포함 → 인덱스만으로 못 끝남

| offset (순위) | Redis | mysql (커버링) | mysql-score-only | 커버링 / Redis | score-only / Redis |
|---:|---:|---:|---:|---:|---:|
| 500 | 0.23ms | 0.67ms | 1.05ms | 3배 | 5배 |
| 5,000 | 0.23ms | 1.16ms | **197ms** | 5배 | 874배 |
| 50,000 | 0.24ms | 5.7ms | 251ms | 24배 | 1,034배 |
| 100,000 | 0.25ms | 10.4ms | 268ms | 43배 | 1,092배 |
| 500,000 | 0.24ms | 51.6ms | 329ms | 216배 | 1,377배 |
| 900,000 | 0.25ms | 92.6ms | 345ms | 372배 | 1,385배 |

Redis와 mysql(커버링) 결과는 offset 8곳 모두 일치. 원본: `results/20261017-0034-users1000000-deep-page.md`

### 해석

1. **방향은 외부 자료와 같다.** Redis는 offset과 무관하게 일정하고(skiplist span으로 O(log N) 점프), MySQL은 offset만큼 읽고 버려서 offset에 비례해 느려진다. 외부 자료의 "offset이 작을 땐 2~3배, 커지면 수백 배"도 커버링 기준 3배 → 372배로 재현됐다.
2. **절대값은 다르다.** 외부 자료 Redis 1.9ms는 여기서 0.25ms다. 측정 클라이언트(화면 표시 포함 여부 등) 차이로 보이고, 확인할 수 없다. 배수로만 비교한다.
3. **"5천 등부터 급격히 느려진다"는 실행 계획이 바뀌기 때문이다.** score-only 테이블에서 `EXPLAIN`을 offset별로 보면:

   | offset | type | key | Extra |
   |---:|---|---|---|
   | 500 ~ 2,000 | index | ix_score | Backward index scan |
   | 3,000 이상 | **ALL** | — | **Using filesort** |

   `KEY(score)`만 있으면 행마다 username을 읽으러 PK(클러스터드 인덱스)를 다시 찾아가야 한다. offset이 커지면 옵티마이저가 "보조 인덱스 + 행마다 PK 조회"보다 "전체 스캔 + 정렬"이 싸다고 판단해 계획을 바꾼다. 그래서 1ms → 197ms로 계단처럼 뛴다. 커버링 인덱스(`score, user_id`)는 끝까지 인덱스만 읽어서(`Using index`) 같은 offset 90만에서도 93ms다.
4. **MySQL에서 깊은 페이지가 필요하면** offset 대신 **키셋 페이지네이션**으로 바꾼다: 직전 페이지 마지막 행의 (score, user_id)를 받아 `WHERE score < @s OR (score = @s AND user_id < @u) ORDER BY score DESC, user_id DESC LIMIT 10`. 인덱스에서 그 위치를 바로 찾으니 "다음 페이지"는 offset과 무관해진다. 대신 "N번째 페이지로 바로 이동"은 안 된다. 바로 이동이 필요한 랭킹이면 Redis다. (키셋 방식은 측정하지 않았다. 행 생성자 비교 `(score, user_id) < (…)`는 MySQL이 인덱스를 잘 못 써서 `OR`로 풀어 쓴다 — `GetAroundAsync`와 같은 방식)

## 순위 테이블 배치 재계산 + 바꿔치기 — 100만 명, 2026-10-18

MySQL만으로 랭킹을 할 때 흔한 방식. 점수는 원본 `ranking`에 쌓고, 주기적으로 순위를 계산해 순위 테이블에 넣는다. 게임은 순위 테이블만 조회한다.

- 순위 테이블: `ranking_snapshot(user_id PK, score, rnk UNIQUE)`
- 재계산: `INSERT INTO ranking_snapshot_new SELECT user_id, score, ROW_NUMBER() OVER (ORDER BY score DESC, user_id DESC) FROM ranking` → `ANALYZE TABLE` → `RENAME TABLE ranking_snapshot TO …_old, ranking_snapshot_new TO ranking_snapshot` (여러 테이블 RENAME은 한 문장이라 원자적이다)
- 조회: 내 순위는 PK, 상위 N명·깊은 페이지·주변은 `rnk BETWEEN` 범위

원본: `results/20261018-2317-users1000000-snapshot.md`. Redis와 결과 일치(순위 200건, 상위 100명, 주변 20건, 깊은 페이지) 불일치 0건.

### 조회 속도 (순차 평균 ms / 32개 동시 초당 처리)

| 연산 | Redis | MySQL 실시간 COUNT | MySQL 순위 테이블 |
|---|---:|---:|---:|
| 내 순위 (무작위 유저) | 0.27 | 34.8 | 0.67 |
| 내 순위 (하위 1%) | 0.27 | 66.1 | 0.65 |
| 상위 100명 | 0.35 | 0.70 | 0.80 |
| 내 주변 ±5명 | 0.53 | 38.0 | 1.34 |
| 깊은 페이지 (90만 등부터 10명) | 0.25 | 94.5 | 0.69 |
| 내 순위 동시 초당 처리 | 48,005 | **250** | 13,521 |

### 재계산 방식별 비교 — 재계산 중 순위 조회 8개 작업이 계속 돈다

| 방식 | 재계산 (부하 없음 / 있음) | 조회 실패 (못 찾음) | 조회 최대 지연 |
|---|---:|---:|---:|
| 새 테이블 + RENAME 바꿔치기 | 3.6s / 9.4s | 0 | 24ms |
| TRUNCATE 후 다시 채우기 | 3.7s / 8.5s | **83,707건 / 94,596건** | 32ms |
| 한 트랜잭션에서 DELETE 후 다시 채우기 | 11.1s / 31.6s | 0 | 16ms |
| 바꿔치기 + 순위 테이블을 읽은 트랜잭션이 RENAME을 2초 막음 | — | 0 | **2,064ms** |

### 재계산 중 원본 점수 갱신 — 갱신 8개 작업이 계속 돈다 (바꿔치기 방식)

| 재계산 세션 격리 수준 | 재계산 중 갱신 수 | 오류 | 최대 지연 |
|---|---:|---:|---:|
| REPEATABLE READ (MySQL 기본) | 1,616 | **데드락 6건** | **4,490ms** |
| READ COMMITTED | 7,769 | 0 | 69ms |

### 해석

1. **효과는 "순위를 미리 계산"에서 나온다.** 내 순위가 35~66ms → 0.65ms, 동시 처리는 250 → 13,521건/초. 깊은 페이지도 `OFFSET` 대신 `rnk` 범위라 순위와 무관하다. Redis와는 2~3배 차이(왕복과 SQL 처리 오버헤드)라 실시간성만 포기하면 충분히 쓸 만하다. 대가는 **재계산 주기만큼 순위가 늦다**는 것.
2. **바꿔치기는 속도가 아니라 "조회 안전성" 때문이다.** TRUNCATE 후 채우면 채우는 동안 순위 테이블이 비어서 그 사이 조회의 88%가 "랭킹에 없음"이었다. 바꿔치기는 조회가 항상 이전 아니면 새 테이블을 봐서 실패 0건.
3. **MySQL(InnoDB)은 한 트랜잭션 DELETE + INSERT도 조회 실패가 없다.** MVCC라 커밋 전까지 조회는 이전 행을 읽는다. 대신 행마다 undo를 남겨서 3배 느리고(11s), 그동안 undo 로그가 커진다. MSSQL 기본 설정(READ COMMITTED, 스냅숏 격리 꺼짐)에서는 이 방식이면 조회가 커밋까지 잠금에 막힌다. MSSQL에서 리네임 방식을 쓴 이유가 이쪽일 가능성이 크다(MSSQL은 측정하지 않았다).
4. **RENAME은 메타데이터 잠금(MDL)이 필요하다.** 순위 테이블을 읽은 트랜잭션이 열려 있으면 RENAME이 기다리고, 그 뒤에 온 조회도 전부 RENAME 뒤에 줄을 선다. 2초 막히면 조회도 2초 멈췄다. 순위 테이블을 오래 읽는 정산·통계 쿼리를 같은 시각에 돌리지 않고, `lock_wait_timeout`을 짧게 걸어 실패하면 재시도한다(기본값 1년).
5. **재계산 세션은 READ COMMITTED로 돌린다.** 기본(REPEATABLE READ + binlog)에서는 `INSERT … SELECT`가 원본 행마다 공유 잠금을 걸어서 재계산 내내 점수 갱신이 막히고(최대 4.5초) 데드락도 났다. READ COMMITTED면 원본을 잠금 없이 읽는다.
6. **바꿔치기 전에 `ANALYZE TABLE`을 한다.** 새로 채운 테이블은 통계가 "빈 테이블"(rows=21)이라 옵티마이저가 `rnk` 인덱스를 두고 전체 스캔 + filesort를 골랐다. 상위 100명 첫 조회가 95ms, InnoDB가 백그라운드에서 통계를 다시 계산하기 전까지 느리다(10만 명에서는 측정 내내 10ms로 안 바뀜).

## 설계로 옮기면

- **실시간 순위**(내 순위, 주변 순위를 자주 묻는 랭킹)는 Redis ZSET이 맞다. MySQL로 같은 일을 하면 인원에 비례해 느려진다.
- **원본과 정산은 MySQL**에 둔다. Redis는 AOF를 켜도 마지막 1초 유실 가능성(everysec)이 있고, 보상 지급 근거는 트랜잭션이 되는 DB에 있어야 한다.
  - 점수 변경 → DB 저장 → Redis `ZADD`. Redis가 날아가면 DB에서 다시 채운다. 이번 측정에서 100만 명 적재는 Redis 5.5초였다.
  - 시즌 종료 정산은 DB에서 윈도 함수로 한 번 계산해 결과 테이블에 저장하고, 보상은 그 테이블 기준으로 지급한다.
- **MySQL만 써야 한다면** 순위를 실시간으로 계산하지 않는다. 주기적으로(예: 5분, 일 단위) 순위 테이블을 배치로 만들고 조회는 그 테이블에서 PK로 읽는다. 상위 N명은 인덱스만으로 실시간 조회해도 된다. 재계산은 새 테이블 + `ANALYZE` + `RENAME` 바꿔치기, 세션은 READ COMMITTED(위 "순위 테이블 배치 재계산" 참고).
- **동점 처리**는 정책이 먼저다. 여기서는 user_id로 정했지만 보통은 "먼저 달성한 사람 우선"이다. Redis는 점수 하나에 정렬 기준이 하나라서 `점수 × 2^k + (기준 시각 − 달성 시각)`처럼 합성 점수로 넣는다. double이라 정수로 정확한 범위는 2^53까지다.
- **더 커지면** (수천만 명, 키 하나가 수 GB) 점수 구간이나 서버별로 키를 나누고, 하위 구간은 "상위 x%" 같은 근사 순위로 보여 준다. 10만 명 ZSET이 약 11MB(`MEMORY USAGE`)였으니 100만 명은 100MB 남짓이다.

## Kafka → Redis 랭킹 반영 (`src/Ranking.Kafka`)

게임 서버가 "점수 획득" 이벤트를 Kafka에 발행하고, 랭킹 갱신기(컨슈머 그룹)가 받아서 Redis ZSET에 더하는 구조. Kafka 4.3.1(KRaft 단일 노드, ZooKeeper 없음)을 WSL에 서비스로 설치했다.

```bash
dotnet run -c Release --project src/Ranking.Kafka -- --users 10000 --events 200000
```

| 항목 | 구현 |
|---|---|
| 토픽 | `score-events`, 파티션 4개 |
| 키 | user_id → 같은 유저는 항상 같은 파티션 → **유저별 순서 보장** (파티션 = 레인 고정과 같은 개념) |
| 프로듀서 | `acks=all`, `enable.idempotence=true` (재전송으로 브로커에 중복이 쌓이는 것을 막음). 로컬 큐가 차면 `Poll` 후 재시도 |
| 컨슈머 | 자동 커밋 끔. 500건 배치로 받아 Redis에 파이프라인으로 반영 → **반영이 끝난 뒤 커밋** (at-least-once) |
| 멱등 반영 | 이벤트에 유저별 seq를 붙이고, Lua 스크립트로 "마지막 seq보다 클 때만 ZINCRBY + seq 기록"을 원자적으로 실행 |

### 결과 (2026-10-16, 이벤트 20만 건, 유저 1만 명)

1. 발행: 초당 약 14만~23만 건(실행마다 차이), 전송 실패 0건. 파티션별 약 5만 건씩 고르게 나뉨
2. 컨슈머 A가 3만 건을 Redis에 반영하고 2만 건까지만 커밋한 뒤 종료 (장애 재현)
3. 같은 그룹으로 B1·B2를 시작 → 커밋 위치부터 다시 받아 **1만 건을 다시 받음**. 멱등 반영이 1만 건을 전부 걸러냄. 한 컨슈머 안에서 유저별 순서 위반 0건. Redis 반영과 커밋을 포함해 초당 약 1만 6천 건
   - 이번 실행에서는 B2가 먼저 합류해 파티션 4개를 다 처리했고 B1은 받은 게 없었다. 파티션이 둘로 나뉘는 모습은 이번 실행에서 확인하지 못했다
4. 최종 점수 검증:

| 반영 방식 | 점수가 틀린 유저 | 초과 지급된 점수 합 |
|---|---:|---:|
| seq 확인 후 반영 (Lua, 멱등) | 0 | 0 |
| 받은 대로 ZINCRBY | 2,446 | 505,629 |

### 면접에서 말할 포인트

- Kafka는 기본이 **at-least-once**다. "반영 → 커밋" 사이에 죽으면 다시 받는다. 순서를 "커밋 → 반영"으로 바꾸면 at-most-once라 유실된다. 그래서 반영 쪽을 멱등으로 만든다.
- 프로듀서 멱등성(`enable.idempotence`)은 **브로커에 같은 메시지가 두 번 쌓이는 것**만 막는다. 컨슈머가 다시 받는 중복은 애플리케이션이 막아야 한다.
- 멱등 키는 유저별 seq로 했다. 유저를 소유한 서버 하나가 seq를 매기고, 같은 유저는 같은 파티션이라 순서대로 오기 때문에 "마지막 seq보다 작거나 같으면 버림"만으로 충분하다. 이벤트 ID를 따로 저장하는 방식보다 저장 공간이 유저 수만큼만 든다.
- 확인과 반영은 Lua 스크립트 하나로 원자적으로 했다. 둘을 따로 보내면 그 사이에 다른 컨슈머가 끼어들 수 있다(리밸런스 직후).
- 보상 지급처럼 돈이 걸린 처리는 같은 원리를 DB에 적용한다: 유니크 키(이벤트 ID) + 같은 트랜잭션에서 반영과 처리 기록.

## 측정의 한계

- 클라이언트와 서버가 같은 PC라 네트워크 지연이 실제보다 작고, CPU를 서로 나눠 쓴다.
- 두 서버 모두 기본 설정이다. MySQL buffer pool(128MB)은 100만 명 테이블이 다 들어가는 크기라 디스크 읽기는 거의 없었을 것이다(확인은 안 했다).
- 각 조건을 한 번씩만 측정했다. 수치는 차이의 크기(몇 배인지)를 보는 용도로만 쓴다.
