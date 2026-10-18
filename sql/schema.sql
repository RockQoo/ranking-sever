-- 랭킹 DB 스키마 (MySQL 8.0.19+ / InnoDB)
-- 적용: wsl -d Ubuntu -- bash scripts/apply-sql.sh
-- IF NOT EXISTS라 여러 번 적용해도 되지만, 이미 있는 테이블의 구조를 바꾸지는 않는다.
-- 구조를 바꿨으면 해당 테이블을 DROP한 뒤 다시 적용한다.
--
-- 정렬 기준(모든 조회 공통): 점수 내림차순, 동점이면 user_id 내림차순

-- 원본 점수. 점수 갱신은 여기에 한다.
CREATE TABLE IF NOT EXISTS ranking (
    user_id INT    NOT NULL PRIMARY KEY,
    score   BIGINT NOT NULL,
    -- 정렬 기준과 같은 순서의 커버링 인덱스.
    -- 상위 N명(역방향 스캔), 내 순위(COUNT 범위 스캔), 페이지(LIMIT OFFSET)가 테이블 본체 없이 인덱스만 읽는다.
    KEY ix_score (score, user_id)
) ENGINE=InnoDB;

-- 배치로 미리 계산한 순위. 게임은 이 테이블만 조회한다 (sp_snapshot_rebuild_* 로 다시 채움).
CREATE TABLE IF NOT EXISTS ranking_snapshot (
    user_id INT    NOT NULL PRIMARY KEY,   -- 내 순위: PK로 바로 찾는다
    score   BIGINT NOT NULL,
    rnk     BIGINT NOT NULL,
    UNIQUE KEY ix_rnk (rnk)                -- 상위 N명·페이지·주변: rnk BETWEEN 범위
) ENGINE=InnoDB;

-- 유저 정보. 랭킹 화면의 닉네임은 여기서 JOIN 한다.
CREATE TABLE IF NOT EXISTS users (
    user_id  INT         NOT NULL PRIMARY KEY,
    nickname VARCHAR(32) NOT NULL
) ENGINE=InnoDB;
