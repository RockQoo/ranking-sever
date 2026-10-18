# ranking-server

게임 랭킹을 **Redis Sorted Set**과 **MySQL(InnoDB)** 으로 각각 구현해 속도를 비교한 C#(.NET 10) 프로젝트.
MySQL은 실시간 계산과 **배치로 미리 계산한 순위 테이블** 두 방식을 쟀고, Kafka → Redis 멱등 반영 데모도 있다.

## 결론

| 100만 명 기준 | Redis | MySQL 실시간 | MySQL 순위 테이블 (배치) |
|---|---:|---:|---:|
| 내 순위 | 0.25ms | 33~66ms | 0.67ms |
| 상위 100명 | 0.3ms | 0.7ms | 0.8ms |
| 깊은 페이지 (90만 등부터) | 0.25ms | 93ms | 0.69ms |
| 내 순위 동시 처리 (32개) | 초당 약 5만 | **초당 250** | 초당 1.3만 |

- **실시간 순위는 Redis.** ZSET skiplist는 순위를 O(log N)에 구한다. InnoDB B+Tree는 앞에 있는 행을 세야 해서 O(순위)다.
- **MySQL만 쓴다면 순위 테이블을 배치로 만들어 바꿔치기한다.** Redis와 2~4배 차이까지 좁혀지는 대신, 재계산 주기만큼 순위가 늦게 반영된다.
  - 새 테이블에 채우기 → `ANALYZE TABLE` → `RENAME TABLE` 한 문장으로 교체하면, 재계산 중에도 조회 실패가 0건이다. TRUNCATE 후 채우면 그 사이 조회의 88%가 실패했다.
  - 재계산 세션은 READ COMMITTED로 돌린다. 기본값(REPEATABLE READ)이면 재계산 내내 점수 갱신이 막히고(최대 4.5초) 데드락도 났다.
- **원본과 정산은 DB에 둔다.** Redis 기본 설정은 매 쓰기를 디스크에 남기지 않는다. 쓰기 속도 차이의 큰 부분은 이 영속화 수준 차이다.
- **Kafka로 반영하면 중복은 반드시 온다**(at-least-once). 유저별 seq + Lua 스크립트로 반영을 멱등으로 만들면 점수 오류가 0건이고, 그냥 반영하면 2,446명이 틀렸다.

자세한 수치와 해석은 [docs/측정결과.md](docs/측정결과.md)에 있다. 브라우저로 보려면 `docs/측정결과.html`을 연다. 측정 원본은 [results/](results/)에 있다.

## 구성

| 경로 | 내용 |
|---|---|
| `src/Ranking.Core` | `IRankingStore`와 구현 3개: Redis, MySQL 실시간, MySQL 순위 테이블 |
| `src/Ranking.Bench` | 벤치마크. 저장소끼리 결과가 같은지 확인한 뒤 측정 |
| `src/Ranking.Api` | Minimal API 랭킹 서버 (`Ranking:Backend=redis\|mysql`) |
| `src/Ranking.Kafka` | Kafka → Redis 멱등 반영 데모 |

## 실행

처음이면 [SETUP.md](SETUP.md)대로 셋팅한다. WSL에 Redis·MySQL·Kafka를 설치하는 건 스크립트 하나로 끝난다.

```bash
dotnet build -c Release
dotnet run -c Release --no-build --project src/Ranking.Bench -- --users 1000000              # 기본 비교
dotnet run -c Release --no-build --project src/Ranking.Bench -- --deep-page --users 1000000  # 깊은 페이지
dotnet run -c Release --no-build --project src/Ranking.Bench -- --snapshot --users 1000000   # 순위 테이블
dotnet run -c Release --project src/Ranking.Kafka                                            # Kafka 데모
dotnet run --project src/Ranking.Api -- --Ranking:Backend=mysql                              # API
```

측정 환경은 한 PC다(Windows 11 클라이언트, WSL2 Ubuntu 서버). 조건마다 한 번씩만 쟀으니 수치는 몇 배 차이인지를 보는 용도로만 쓴다.
