using System.Data;
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
/// 배치로 미리 계산한 순위 테이블. 점수 갱신은 원본 ranking 테이블에 하고(MySqlRankingStore와 같은 스키마),
/// 조회는 ranking_snapshot(user_id PK, score, rnk UNIQUE)에서 한다.
/// 순위는 RebuildAsync를 부른 시점 기준이라 그 뒤 갱신은 다음 재계산까지 반영되지 않는다.
/// </summary>
public sealed class MySqlSnapshotRankingStore : IRankingStore
{
    private const string Table = "ranking_snapshot";

    private readonly MySqlDataSource _dataSource;
    private readonly MySqlRankingStore _live;

    public string Name => "mysql-snapshot";

    public MySqlSnapshotRankingStore(string connectionString)
    {
        _dataSource = new MySqlDataSource(connectionString);
        _live = new MySqlRankingStore(connectionString);
    }

    private static string CreateTableSql(string name) => $"""
        CREATE TABLE {name} (
            user_id INT    NOT NULL PRIMARY KEY,
            score   BIGINT NOT NULL,
            rnk     BIGINT NOT NULL,
            UNIQUE KEY ix_rnk (rnk)
        ) ENGINE=InnoDB
        """;

    // 정렬 기준은 다른 구현과 같다: 점수 내림차순, 동점이면 user_id 내림차순
    private static string FillSql(string name) => $"""
        INSERT INTO {name} (user_id, score, rnk)
        SELECT user_id, score, ROW_NUMBER() OVER (ORDER BY score DESC, user_id DESC)
        FROM ranking
        """;

    public async Task ResetAsync()
    {
        await _live.ResetAsync();
        await using var conn = await _dataSource.OpenConnectionAsync();
        await ExecuteAsync(conn, $"DROP TABLE IF EXISTS {Table}, {Table}_new, {Table}_old");
        await ExecuteAsync(conn, CreateTableSql(Table));
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
        await using var conn = await _dataSource.OpenConnectionAsync();
        if (readCommitted)
            await ExecuteAsync(conn, "SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED");

        switch (mode)
        {
            case SnapshotRebuildMode.Swap:
                await ExecuteAsync(conn, $"DROP TABLE IF EXISTS {Table}_new, {Table}_old");
                await ExecuteAsync(conn, CreateTableSql(Table + "_new"));
                await ExecuteAsync(conn, FillSql(Table + "_new"));
                if (analyze)
                    await ExecuteAsync(conn, $"ANALYZE TABLE {Table}_new");
                // 여러 테이블 RENAME은 원자적이다. 조회는 이전 테이블 아니면 새 테이블을 보고, 그 사이는 없다.
                await ExecuteAsync(conn, $"RENAME TABLE {Table} TO {Table}_old, {Table}_new TO {Table}");
                await ExecuteAsync(conn, $"DROP TABLE {Table}_old");
                break;

            case SnapshotRebuildMode.Truncate:
                await ExecuteAsync(conn, $"TRUNCATE TABLE {Table}");
                await ExecuteAsync(conn, FillSql(Table));
                if (analyze)
                    await ExecuteAsync(conn, $"ANALYZE TABLE {Table}");
                break;

            case SnapshotRebuildMode.DeleteInTransaction:
                await using (var tx = await conn.BeginTransactionAsync(readCommitted ? IsolationLevel.ReadCommitted : IsolationLevel.RepeatableRead))
                {
                    await ExecuteAsync(conn, $"DELETE FROM {Table}", tx);
                    await ExecuteAsync(conn, FillSql(Table), tx);
                    await tx.CommitAsync();
                }
                if (analyze)
                    await ExecuteAsync(conn, $"ANALYZE TABLE {Table}");
                break;
        }
    }

    public async Task<long?> GetRankAsync(int userId)
    {
        await using var conn = await _dataSource.OpenConnectionAsync();
        await using var cmd = new MySqlCommand($"SELECT rnk FROM {Table} WHERE user_id = @u", conn);
        cmd.Parameters.AddWithValue("@u", userId);
        var result = await cmd.ExecuteScalarAsync();
        return result is null or DBNull ? null : Convert.ToInt64(result);
    }

    public Task<IReadOnlyList<RankEntry>> GetTopAsync(int n) => RangeAsync(1, n);

    // OFFSET 대신 rnk 범위로 찾으니 깊은 페이지도 인덱스에서 바로 그 위치로 간다.
    public Task<IReadOnlyList<RankEntry>> GetPageAsync(long offset, int count) => RangeAsync(offset + 1, offset + count);

    public async Task<IReadOnlyList<RankEntry>> GetAroundAsync(int userId, int range)
    {
        await using var conn = await _dataSource.OpenConnectionAsync();
        await using var cmd = new MySqlCommand($"""
            SELECT s.user_id, s.score, s.rnk
            FROM {Table} me
            JOIN {Table} s ON s.rnk BETWEEN me.rnk - @k AND me.rnk + @k
            WHERE me.user_id = @u
            ORDER BY s.rnk
            """, conn);
        cmd.Parameters.AddWithValue("@u", userId);
        cmd.Parameters.AddWithValue("@k", range);
        return await ReadEntriesAsync(cmd, range * 2 + 1);
    }

    public async Task<long> CountAsync()
    {
        await using var conn = await _dataSource.OpenConnectionAsync();
        await using var cmd = new MySqlCommand($"SELECT COUNT(*) FROM {Table}", conn);
        return Convert.ToInt64(await cmd.ExecuteScalarAsync());
    }

    private async Task<IReadOnlyList<RankEntry>> RangeAsync(long fromRank, long toRank)
    {
        await using var conn = await _dataSource.OpenConnectionAsync();
        await using var cmd = new MySqlCommand(
            $"SELECT user_id, score, rnk FROM {Table} WHERE rnk BETWEEN @from AND @to ORDER BY rnk", conn);
        cmd.Parameters.AddWithValue("@from", fromRank);
        cmd.Parameters.AddWithValue("@to", toRank);
        return await ReadEntriesAsync(cmd, (int)(toRank - fromRank + 1));
    }

    private static async Task<IReadOnlyList<RankEntry>> ReadEntriesAsync(MySqlCommand cmd, int capacity)
    {
        var result = new List<RankEntry>(capacity);
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
            result.Add(new RankEntry(reader.GetInt32(0), reader.GetInt64(1), reader.GetInt64(2)));
        return result;
    }

    private static async Task ExecuteAsync(MySqlConnection conn, string sql, MySqlTransaction? tx = null)
    {
        await using var cmd = new MySqlCommand(sql, conn, tx) { CommandTimeout = 0 };
        await cmd.ExecuteNonQueryAsync();
    }

    public async ValueTask DisposeAsync()
    {
        await _live.DisposeAsync();
        await _dataSource.DisposeAsync();
    }
}
