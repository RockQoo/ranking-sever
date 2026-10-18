using System.Text;
using MySqlConnector;

namespace Ranking.Core;

/// <summary>내 순위를 MySQL에서 구하는 방식.</summary>
public enum MySqlRankMode
{
    /// <summary>나보다 앞선 행 수를 (score, user_id) 인덱스로 센다. 비용은 내 순위에 비례(O(rank)).</summary>
    Count,

    /// <summary>ROW_NUMBER() 윈도 함수로 전체를 정렬한 뒤 나를 찾는다. 매번 전체 정렬(O(N log N)).</summary>
    Window,
}

/// <summary>
/// MySQL(InnoDB) 랭킹, 실시간 계산. 테이블은 sql/schema.sql의 ranking, 쿼리는 sql/procedures.sql의 sp_ranking_*.
/// (score, user_id) 보조 인덱스 하나로 상위 N명은 인덱스 역방향 스캔, 내 순위는 COUNT로 구한다.
/// </summary>
public sealed class MySqlRankingStore : IRankingStore
{
    private readonly MySqlDataSource _dataSource;
    private readonly MySqlRankMode _rankMode;

    public string Name => _rankMode == MySqlRankMode.Count ? "mysql" : "mysql-window";

    public MySqlRankingStore(string connectionString, MySqlRankMode rankMode = MySqlRankMode.Count)
    {
        _dataSource = new MySqlDataSource(connectionString);
        _rankMode = rankMode;
    }

    public Task ResetAsync() => MySqlProc.ExecuteAsync(_dataSource, "CALL sp_ranking_reset()");

    // 대량 적재는 게임 서버가 아니라 도구가 하는 일이라 SP 대신 다중 행 INSERT로 넣는다.
    public async Task BulkLoadAsync(IReadOnlyList<(int UserId, long Score)> rows)
    {
        const int batchSize = 5_000;
        await using var conn = await _dataSource.OpenConnectionAsync();
        await using var tx = await conn.BeginTransactionAsync();

        var sql = new StringBuilder();
        for (var i = 0; i < rows.Count; i += batchSize)
        {
            var count = Math.Min(batchSize, rows.Count - i);
            sql.Clear().Append("INSERT INTO ranking (user_id, score) VALUES ");
            for (var j = 0; j < count; ++j)
            {
                var (userId, score) = rows[i + j];
                if (j > 0)
                    sql.Append(',');
                sql.Append('(').Append(userId).Append(',').Append(score).Append(')');
            }
            await using var cmd = new MySqlCommand(sql.ToString(), conn, tx);
            await cmd.ExecuteNonQueryAsync();
        }

        await tx.CommitAsync();
        await using var analyze = new MySqlCommand("ANALYZE TABLE ranking", conn);
        await analyze.ExecuteNonQueryAsync();
    }

    public Task SetScoreAsync(int userId, long score) =>
        MySqlProc.ExecuteAsync(_dataSource, "CALL sp_ranking_set_score(@u, @s)", ("@u", userId), ("@s", score));

    public async Task<long?> GetRankAsync(int userId)
    {
        var call = _rankMode == MySqlRankMode.Count ? "CALL sp_ranking_get_rank(@u)" : "CALL sp_ranking_get_rank_window(@u)";
        var result = await MySqlProc.ScalarAsync(_dataSource, call, ("@u", userId));
        return result is null ? null : Convert.ToInt64(result);
    }

    public async Task<IReadOnlyList<RankEntry>> GetTopAsync(int n)
    {
        var rows = await MySqlProc.ReadAsync(_dataSource, "CALL sp_ranking_get_top(@n)",
            r => (UserId: r.GetInt32(0), Score: r.GetInt64(1)), ("@n", n));
        return rows.Select((row, i) => new RankEntry(row.UserId, row.Score, i + 1)).ToList();
    }

    // LIMIT ... OFFSET은 앞의 offset개를 인덱스에서 읽고 버린다. 비용은 offset에 비례.
    public async Task<IReadOnlyList<RankEntry>> GetPageAsync(long offset, int count)
    {
        var rows = await MySqlProc.ReadAsync(_dataSource, "CALL sp_ranking_get_page(@o, @n)",
            r => (UserId: r.GetInt32(0), Score: r.GetInt64(1)), ("@o", offset), ("@n", count));
        return rows.Select((row, i) => new RankEntry(row.UserId, row.Score, offset + i + 1)).ToList();
    }

    // 내 순위 COUNT와 위·아래 range명 조회를 SP 하나에서 한다 (1회 왕복).
    public async Task<IReadOnlyList<RankEntry>> GetAroundAsync(int userId, int range) =>
        await MySqlProc.ReadAsync(_dataSource, "CALL sp_ranking_get_around(@u, @k)",
            r => new RankEntry(r.GetInt32(0), r.GetInt64(1), r.GetInt64(2)), ("@u", userId), ("@k", range));

    public async Task<long> CountAsync() =>
        Convert.ToInt64(await MySqlProc.ScalarAsync(_dataSource, "CALL sp_ranking_count()"));

    public ValueTask DisposeAsync() => _dataSource.DisposeAsync();
}
