# 셋팅 가이드

Windows 11 PC에서 이 저장소를 받아 벤치마크와 API를 돌릴 수 있게 만드는 순서. 클라이언트(.NET)는 Windows에서, 서버(Redis·MySQL·Kafka)는 WSL2 Ubuntu에서 돈다.

> **AI로 셋팅하기:** 저장소를 받은 폴더에서 Claude Code를 열고 "SETUP.md대로 셋팅해줘"라고 하면 된다. 각 단계에 확인 명령이 있어서 AI가 이미 된 단계는 건너뛰고 진행한다. 관리자 권한이 필요하거나 재부팅해야 하는 단계(WSL 설치)는 사람이 해야 한다.

## 측정에 쓴 버전

| 항목 | 버전 |
|---|---|
| Windows | 11 |
| .NET SDK | 10 |
| WSL 배포판 | Ubuntu 26.04 (이름 `Ubuntu`) |
| Redis | 8.0.5 |
| MySQL | 8.4.11 (8.0.19 이상이면 동작) |
| Kafka | 4.3.1, KRaft 단일 노드, OpenJDK 21 |

다른 Ubuntu 버전이면 apt가 주는 Redis·MySQL 버전이 달라진다. 동작은 하지만 수치는 README와 다를 수 있다.

## 1. Windows 준비

| 항목 | 확인 명령 | 없으면 |
|---|---|---|
| WSL2 + Ubuntu | `wsl -l -v` → `Ubuntu`, VERSION `2` | 관리자 PowerShell에서 `wsl --install -d Ubuntu` → 재부팅 → 처음 실행해서 리눅스 계정 만들기 |
| .NET 10 SDK | `dotnet --list-sdks` → `10.x` | `winget install Microsoft.DotNet.SDK.10` |
| Git | `git --version` | `winget install Git.Git` |

배포판 이름이 `Ubuntu`가 아니면(예: `Ubuntu-24.04`) 아래 명령의 `-d Ubuntu`를 그 이름으로 바꾼다.

## 2. 저장소 받기

```bash
git clone https://github.com/RockQoo/ranking-sever.git ranking-server
cd ranking-server
```

## 3. WSL에 Redis·MySQL·Kafka 설치

저장소 폴더에서 실행한다. WSL은 현재 Windows 폴더를 `/mnt/c/...`로 열어서 스크립트를 바로 찾는다.

```bash
wsl -d Ubuntu -u root -- bash scripts/setup-wsl.sh --with-kafka
```

- Kafka 데모를 안 돌리면 `--with-kafka`를 빼도 된다.
- 하는 일: Redis, MySQL 설치와 서비스 등록 → DB `ranking`, 계정 `bench`/`bench` 생성 → (선택) Kafka를 `/opt/kafka`에 받아 `127.0.0.1:9092`로 광고하게 설정하고, systemd 서비스 `kafka`로 등록한다.
- 여러 번 실행해도 된다. 이미 설치된 패키지는 버전을 바꾸지 않는다.
- 끝에 `PONG`, `mysql ok`, `kafka ok`, `완료`가 나오면 성공이다.
- "systemd가 꺼져 있다"가 나오면 WSL 안의 `/etc/wsl.conf`에 아래를 넣는다. 그다음 Windows에서 `wsl --shutdown`을 하고 다시 실행한다.
  ```ini
  [boot]
  systemd=true
  ```

## 4. 빌드와 동작 확인

WSL은 쓰는 프로세스가 없으면 몇 초 뒤 꺼지고 Redis·MySQL도 같이 멈춘다. **다른 창에서 먼저 아래를 켜 둔다**(WSL을 깨우면서 붙잡아 둔다).

```bash
wsl -d Ubuntu -- sleep 3600
```

```bash
dotnet build -c Release

# 1만 명으로 짧게. Redis와 MySQL 결과가 같은지 확인한 뒤 측정한다 (1분 안쪽)
dotnet run -c Release --no-build --project src/Ranking.Bench -- --users 10000 --ops 100 --throughput-ops 1000
```

- `불일치 0건`이 나오고 끝에 `결과 저장: …\results\…md`가 나오면 성공이다. 확인용 결과 파일은 지워도 된다.
- 벤치마크는 시작할 때 `ranking` 테이블과 `ranking:season` 키를 **지우고 새로 넣는다**.

본 측정 명령은 [README](README.md#실행)에 있다.

## 5. 데이터 직접 보기

```bash
wsl -d Ubuntu -- redis-cli ZREVRANGE ranking:season 0 9 WITHSCORES
wsl -d Ubuntu -- mysql -ubench -pbench ranking -e "SELECT * FROM ranking ORDER BY score DESC, user_id DESC LIMIT 10"
wsl -d Ubuntu -- mysql -ubench -pbench ranking -e "SELECT * FROM ranking_snapshot WHERE rnk BETWEEN 1 AND 10"   # --snapshot 실행 후
```

Windows에서 GUI 도구(MySQL Workbench, DBeaver, Redis Insight)로 `127.0.0.1`에 붙어도 된다.

## 문제가 생기면

| 증상 | 원인 / 해결 |
|---|---|
| 측정 도중 연결이 끊김 | WSL은 쓰는 프로세스가 없으면 몇 초 뒤 꺼진다. 측정하는 동안 다른 창에서 `wsl -d Ubuntu -- sleep 3600`을 켜 둔다 |
| Windows에서 Kafka에 못 붙음 | `advertised.listeners`가 `localhost`면 클라이언트가 `::1`로 붙으려다 실패한다. 스크립트는 `127.0.0.1`로 설정한다. 서비스의 `KAFKA_OPTS=-Djava.net.preferIPv4Stack=true`도 필요하다 |
| 3306·6379·9092 포트에 엉뚱한 서버가 응답 | Windows에 MySQL이나 Redis가 따로 설치돼 있으면 그쪽이 먼저 받는다. Windows 쪽 서비스를 끈다 |
| 서비스 상태 확인 | `wsl -d Ubuntu -- systemctl is-active redis-server mysql kafka` |
