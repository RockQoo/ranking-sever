-- 랭킹 저장 프로시저. 게임 서버는 CALL 로만 호출한다.
-- 적용: wsl -d Ubuntu -- bash scripts/apply-sql.sh (schema.sql 다음에)
-- 정렬 기준(모든 조회 공통): 점수 내림차순, 동점이면 user_id 내림차순
--
-- (score, user_id) > (s, u) 같은 행 생성자 비교는 MySQL이 인덱스를 잘 못 써서
-- 전부 "score > s OR (score = s AND user_id > u)" 형태로 풀어 쓴다.

DELIMITER $$

-- =====================================================================
-- 원본 ranking: 실시간 계산
-- =====================================================================

DROP PROCEDURE IF EXISTS sp_ranking_reset$$
CREATE PROCEDURE sp_ranking_reset()
BEGIN
    TRUNCATE TABLE ranking;
END$$

DROP PROCEDURE IF EXISTS sp_ranking_count$$
CREATE PROCEDURE sp_ranking_count()
BEGIN
    SELECT COUNT(*) FROM ranking;
END$$

-- 점수 갱신. 없으면 추가한다.
DROP PROCEDURE IF EXISTS sp_ranking_set_score$$
CREATE PROCEDURE sp_ranking_set_score(IN p_user_id INT, IN p_score BIGINT)
BEGIN
    INSERT INTO ranking (user_id, score) VALUES (p_user_id, p_score) AS new
    ON DUPLICATE KEY UPDATE score = new.score;
END$$

-- 내 순위 = 나보다 앞선 사람 수 + 1. 두 COUNT 모두 ix_score 범위 스캔이라 비용은 O(내 순위).
-- 랭킹에 없으면 결과 행이 없다.
DROP PROCEDURE IF EXISTS sp_ranking_get_rank$$
CREATE PROCEDURE sp_ranking_get_rank(IN p_user_id INT)
BEGIN
    SELECT 1 + (SELECT COUNT(*) FROM ranking WHERE score > t.score)
             + (SELECT COUNT(*) FROM ranking WHERE score = t.score AND user_id > t.user_id) AS rnk
    FROM ranking t
    WHERE t.user_id = p_user_id;
END$$

-- 비교용: 윈도 함수로 전체를 정렬한 뒤 나를 찾는다. 매번 O(N log N)이라 요청마다 쓰면 안 된다.
DROP PROCEDURE IF EXISTS sp_ranking_get_rank_window$$
CREATE PROCEDURE sp_ranking_get_rank_window(IN p_user_id INT)
BEGIN
    SELECT rnk FROM (
        SELECT user_id, ROW_NUMBER() OVER (ORDER BY score DESC, user_id DESC) AS rnk
        FROM ranking
    ) x
    WHERE x.user_id = p_user_id;
END$$

-- 상위 n명. ix_score 역방향 스캔으로 n개만 읽는다.
DROP PROCEDURE IF EXISTS sp_ranking_get_top$$
CREATE PROCEDURE sp_ranking_get_top(IN p_n INT)
BEGIN
    SELECT user_id, score FROM ranking
    ORDER BY score DESC, user_id DESC
    LIMIT p_n;
END$$

-- offset(0부터)부터 count명. 앞의 offset개를 인덱스에서 읽고 버려서 비용은 O(offset).
DROP PROCEDURE IF EXISTS sp_ranking_get_page$$
CREATE PROCEDURE sp_ranking_get_page(IN p_offset BIGINT, IN p_count INT)
BEGIN
    SELECT user_id, score FROM ranking
    ORDER BY score DESC, user_id DESC
    LIMIT p_count OFFSET p_offset;
END$$

-- 내 위아래 range명씩 (나 포함), 순위까지. 랭킹에 없으면 결과 행이 없다.
DROP PROCEDURE IF EXISTS sp_ranking_get_around$$
CREATE PROCEDURE sp_ranking_get_around(IN p_user_id INT, IN p_range INT)
proc: BEGIN
    DECLARE v_score BIGINT;
    DECLARE v_rank BIGINT;

    SELECT t.score,
           1 + (SELECT COUNT(*) FROM ranking WHERE score > t.score)
             + (SELECT COUNT(*) FROM ranking WHERE score = t.score AND user_id > t.user_id)
    INTO v_score, v_rank
    FROM ranking t
    WHERE t.user_id = p_user_id;

    IF v_score IS NULL THEN
        LEAVE proc;
    END IF;

    SELECT user_id, score, rnk FROM (
        -- 내 위: 나와 가까운 순(오름차순)으로 range명 → 순위는 내 순위 - 1, -2, …
        SELECT user_id, score, CAST(v_rank AS SIGNED) - CAST(ROW_NUMBER() OVER (ORDER BY score ASC, user_id ASC) AS SIGNED) AS rnk
        FROM (SELECT user_id, score FROM ranking
              WHERE score > v_score OR (score = v_score AND user_id > p_user_id)
              ORDER BY score ASC, user_id ASC
              LIMIT p_range) above
        UNION ALL
        SELECT p_user_id, v_score, v_rank
        UNION ALL
        -- 내 아래
        SELECT user_id, score, CAST(v_rank AS SIGNED) + CAST(ROW_NUMBER() OVER (ORDER BY score DESC, user_id DESC) AS SIGNED)
        FROM (SELECT user_id, score FROM ranking
              WHERE score < v_score OR (score = v_score AND user_id < p_user_id)
              ORDER BY score DESC, user_id DESC
              LIMIT p_range) below
    ) x
    ORDER BY rnk;
END$$

-- 닉네임 포함 페이지 — 단순 JOIN. 버릴 offset개 행마다 users를 PK로 찾아 JOIN한 뒤 버려서 가장 느리다.
DROP PROCEDURE IF EXISTS sp_ranking_get_page_named$$
CREATE PROCEDURE sp_ranking_get_page_named(IN p_offset BIGINT, IN p_count INT)
BEGIN
    SELECT r.user_id, u.nickname, r.score
    FROM ranking r JOIN users u ON u.user_id = r.user_id
    ORDER BY r.score DESC, r.user_id DESC
    LIMIT p_count OFFSET p_offset;
END$$

-- 닉네임 포함 페이지 — 지연 조인. 안쪽은 ix_score만 읽어(커버링) count명을 고르고, 그 count명만 users와 JOIN 한다.
DROP PROCEDURE IF EXISTS sp_ranking_get_page_named_deferred$$
CREATE PROCEDURE sp_ranking_get_page_named_deferred(IN p_offset BIGINT, IN p_count INT)
BEGIN
    SELECT r.user_id, u.nickname, r.score
    FROM (SELECT user_id, score FROM ranking
          ORDER BY score DESC, user_id DESC
          LIMIT p_count OFFSET p_offset) r
    JOIN users u ON u.user_id = r.user_id
    ORDER BY r.score DESC, r.user_id DESC;
END$$

-- =====================================================================
-- 순위 테이블 ranking_snapshot: 배치로 미리 계산
-- 재계산 세션은 READ COMMITTED로 부른다. REPEATABLE READ(기본) + binlog면
-- INSERT … SELECT가 원본 ranking 행마다 공유 잠금을 걸어 재계산 내내 점수 갱신이 막힌다.
-- p_analyze = FALSE는 비교 측정용. 새로 채운 테이블은 통계가 "빈 테이블"이라 rnk 인덱스를 안 쓴다.
-- =====================================================================

DROP PROCEDURE IF EXISTS sp_snapshot_reset$$
CREATE PROCEDURE sp_snapshot_reset()
BEGIN
    DROP TABLE IF EXISTS ranking_snapshot_new, ranking_snapshot_old;
    TRUNCATE TABLE ranking_snapshot;
END$$

-- 새 테이블에 채운 뒤 RENAME 한 문장으로 바꿔치기. 조회는 항상 이전 아니면 새 테이블을 본다.
DROP PROCEDURE IF EXISTS sp_snapshot_rebuild_swap$$
CREATE PROCEDURE sp_snapshot_rebuild_swap(IN p_analyze BOOLEAN)
BEGIN
    DROP TABLE IF EXISTS ranking_snapshot_new, ranking_snapshot_old;
    CREATE TABLE ranking_snapshot_new LIKE ranking_snapshot;
    INSERT INTO ranking_snapshot_new (user_id, score, rnk)
    SELECT user_id, score, ROW_NUMBER() OVER (ORDER BY score DESC, user_id DESC)
    FROM ranking;
    IF p_analyze THEN
        ANALYZE TABLE ranking_snapshot_new;
    END IF;
    -- 여러 테이블 RENAME은 원자적이다. 순위 테이블을 읽은 트랜잭션이 열려 있으면 메타데이터 잠금을 기다린다.
    RENAME TABLE ranking_snapshot TO ranking_snapshot_old, ranking_snapshot_new TO ranking_snapshot;
    DROP TABLE ranking_snapshot_old;
END$$

-- 비교용: 같은 테이블을 비우고 다시 채운다. 채우는 동안 조회하면 비어 있다.
DROP PROCEDURE IF EXISTS sp_snapshot_rebuild_truncate$$
CREATE PROCEDURE sp_snapshot_rebuild_truncate(IN p_analyze BOOLEAN)
BEGIN
    TRUNCATE TABLE ranking_snapshot;
    INSERT INTO ranking_snapshot (user_id, score, rnk)
    SELECT user_id, score, ROW_NUMBER() OVER (ORDER BY score DESC, user_id DESC)
    FROM ranking;
    IF p_analyze THEN
        ANALYZE TABLE ranking_snapshot;
    END IF;
END$$

-- 비교용: 한 트랜잭션에서 지우고 다시 채운다. InnoDB는 MVCC라 커밋 전까지 조회는 이전 행을 보지만 행마다 undo가 쌓여 느리다.
DROP PROCEDURE IF EXISTS sp_snapshot_rebuild_delete$$
CREATE PROCEDURE sp_snapshot_rebuild_delete(IN p_analyze BOOLEAN)
BEGIN
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        ROLLBACK;
        RESIGNAL;
    END;

    START TRANSACTION;
    DELETE FROM ranking_snapshot;
    INSERT INTO ranking_snapshot (user_id, score, rnk)
    SELECT user_id, score, ROW_NUMBER() OVER (ORDER BY score DESC, user_id DESC)
    FROM ranking;
    COMMIT;
    IF p_analyze THEN
        ANALYZE TABLE ranking_snapshot;
    END IF;
END$$

DROP PROCEDURE IF EXISTS sp_snapshot_count$$
CREATE PROCEDURE sp_snapshot_count()
BEGIN
    SELECT COUNT(*) FROM ranking_snapshot;
END$$

-- 내 순위: PK 조회. 랭킹에 없으면 결과 행이 없다.
DROP PROCEDURE IF EXISTS sp_snapshot_get_rank$$
CREATE PROCEDURE sp_snapshot_get_rank(IN p_user_id INT)
BEGIN
    SELECT rnk FROM ranking_snapshot WHERE user_id = p_user_id;
END$$

-- 순위 p_from~p_to (1부터). 상위 N명과 깊은 페이지 모두 rnk 범위라 위치와 무관하다.
DROP PROCEDURE IF EXISTS sp_snapshot_get_range$$
CREATE PROCEDURE sp_snapshot_get_range(IN p_from BIGINT, IN p_to BIGINT)
BEGIN
    SELECT user_id, score, rnk FROM ranking_snapshot
    WHERE rnk BETWEEN p_from AND p_to
    ORDER BY rnk;
END$$

-- 내 위아래 range명씩 (나 포함). 랭킹에 없으면 결과 행이 없다.
DROP PROCEDURE IF EXISTS sp_snapshot_get_around$$
CREATE PROCEDURE sp_snapshot_get_around(IN p_user_id INT, IN p_range INT)
BEGIN
    SELECT s.user_id, s.score, s.rnk
    FROM ranking_snapshot me
    JOIN ranking_snapshot s ON s.rnk BETWEEN me.rnk - p_range AND me.rnk + p_range
    WHERE me.user_id = p_user_id
    ORDER BY s.rnk;
END$$

-- 닉네임 포함 페이지. offset(0부터)부터 count명.
DROP PROCEDURE IF EXISTS sp_snapshot_get_page_named$$
CREATE PROCEDURE sp_snapshot_get_page_named(IN p_offset BIGINT, IN p_count INT)
BEGIN
    SELECT s.user_id, u.nickname, s.score
    FROM ranking_snapshot s JOIN users u ON u.user_id = s.user_id
    WHERE s.rnk BETWEEN p_offset + 1 AND p_offset + p_count
    ORDER BY s.rnk;
END$$

DELIMITER ;
