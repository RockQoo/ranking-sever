using MySqlConnector;

namespace Ranking.Core;

/// <summary>순위 테이블을 다시 채우는 방식.</summary>
public enum SnapshotRebuildMode
{
    /// <summary>새 테이블에 채운 뒤 RENAME TABLE 한 문장으로 바꿔치기. 조회는 항상 완성된 테이블을 본다.</summary>
    Swap,

    /// <summary>같은 테이블을 TRUNCATE 하고 다시 채운다. 채우는 동안 조회하면 비어 있다.</summary>
    Truncate,

    /// <summary>한 트랜잭션 안에서 DELETE 후 다시 채운다. InnoDB는 MVCC라 커밋 전까지 조회는 이전 데이터를 본다.</summary>
    DeleteInTransaction,
}

/// <summary>
/// 배치로 미리 계산한 순위 테이블. 점수 갱신은 원본 ranking 테이블에 하고(MySqlRankingStore와 같음),
/// 조회는 ranking_snapshot(user_id PK, score, rnk UNIQUE)에서 한다. 테이블은 sql/schema.sql, 쿼리는 sp_snapshot_*.
/// 순위는 RebuildAsync를 부른 시점 기준이라 그 뒤 갱신은 다음 재계산까지 반영되지 않는다.
/// </summary>
public sealed class MySqlSnapshotRankingStore : IRankingStore
{
    private readonly MySqlDataSource _dataSource;
    private readonly MySqlRankingStore _live;

    public string Name => "mysql-snapshot";

    public MySqlSnapshotRankingStore(string connectionString)
    {
        _dataSource = new MySqlDataSource(connectionString);
        _live = new MySqlRankingStore(connectionString);
    }

    public async Task ResetAsync()
    {
        await _live.ResetAsync();
        await MySqlProc.ExecuteAsync(_dataSource, "CALL sp_snapshot_reset()");
    }

    public Task BulkLoadAsync(IReadOnlyList<(int UserId, long Score)> rows) => _live.BulkLoadAsync(rows);

    public Task SetScoreAsync(int userId, long score) => _live.SetScoreAsync(userId, score);

    /// <summary>
    /// 원본 ranking에서 순위를 다시 계산한다.
    /// readCommitted가 false면 세션 기본 격리 수준(보통 REPEATABLE READ)으로 읽는데,
    /// 그러면 INSERT … SELECT가 원본 행마다 공유 잠금을 걸어서 재계산이 끝날 때까지 점수 갱신이 막힌다.
    /// analyze가 false면 통계를 갱신하지 않는다. 새로 만든 테이블은 통계가 "빈 테이블"이라
    /// 옵티마이저가 rnk 인덱스를 두고 전체 스캔 + filesort를 고른다 (비교 측정용).
    /// </summary>
    public async Task RebuildAsync(SnapshotRebuildMode mode = SnapshotRebuildMode.Swap, bool readCommitted = true, bool analyze = true)
    {
        var call = mode switch
        {
            SnapshotRebuildMode.Swap => "CALL sp_snapshot_rebuild_swap(@a)",
            SnapshotRebuildMode.Truncate => "CALL sp_snapshot_rebuild_truncate(@a)",
            SnapshotRebuildMode.DeleteInTransaction => "CALL sp_snapshot_rebuild_delete(@a)",
            _ => throw new ArgumentOutOfRangeException(nameof(mode)),
        };

        await using var conn = await _dataSource.OpenConnectionAsync();
        if (readCommitted)
        {
            // 세션 설정이라 이 연결에만 적용된다. 연결이 풀로 돌아갈 때 초기화된다.
            await using var set = new MySqlCommand("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED", conn);
            await set.ExecuteNonQueryAsync();
        }
        await using var cmd = MySqlProc.Command(conn, call, ("@a", analyze));
        await cmd.ExecuteNonQueryAsync();
    }

    public async Task<long?> GetRankAsync(int userId)
    {
        var result = await MySqlProc.ScalarAsync(_dataSource, "CALL sp_snapshot_get_rank(@u)", ("@u", userId));
        return result is null ? null : Convert.ToInt64(result);
    }

    public Task<IReadOnlyList<RankEntry>> GetTopAsync(int n) => RangeAsync(1, n);

    // OFFSET 대신 rnk 범위로 찾으니 깊은 페이지도 인덱스에서 바로 그 위치로 간다.
    public Task<IReadOnlyList<RankEntry>> GetPageAsync(long offset, int count) => RangeAsync(offset + 1, offset + count);

    public async Task<IReadOnlyList<RankEntry>> GetAroundAsync(int userId, int range) =>
        await MySqlProc.ReadAsync(_dataSource, "CALL sp_snapshot_get_around(@u, @k)", ToEntry, ("@u", userId), ("@k", range));

    public async Task<long> CountAsync() =>
        Convert.ToInt64(await MySqlProc.ScalarAsync(_dataSource, "CALL sp_snapshot_count()"));

    private async Task<IReadOnlyList<RankEntry>> RangeAsync(long fromRank, long toRank) =>
        await MySqlProc.ReadAsync(_dataSource, "CALL sp_snapshot_get_range(@from, @to)", ToEntry, ("@from", fromRank), ("@to", toRank));

    private static RankEntry ToEntry(MySqlDataReader r) => new(r.GetInt32(0), r.GetInt64(1), r.GetInt64(2));

    public async ValueTask DisposeAsync()
    {
        await _live.DisposeAsync();
        await _dataSource.DisposeAsync();
    }
}
