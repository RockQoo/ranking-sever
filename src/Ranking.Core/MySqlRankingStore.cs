using System.Text;
using MySqlConnector;

namespace Ranking.Core;

/// <summary>내 순위를 MySQL에서 구하는 방식.</summary>
public enum MySqlRankMode
{
    /// <summary>나보다 앞선 행 수를 (score, user_id) 인덱스로 센다. 비용은 내 순위에 비례(O(rank)).</summary>
    Count,

    /// <summary>RANK() 윈도 함수로 전체를 정렬한 뒤 나를 찾는다. 매번 전체 정렬(O(N log N)).</summary>
    Window,
}

/// <summary>
/// MySQL(InnoDB) 랭킹. (score, user_id) 보조 인덱스 하나로
/// 상위 N명은 인덱스 역방향 스캔, 내 순위는 COUNT로 구한다.
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

    private const string CreateTableSql = """
        CREATE TABLE ranking (
            user_id INT    NOT NULL PRIMARY KEY,
            score   BIGINT NOT NULL,
            KEY ix_score (score, user_id)
        ) ENGINE=InnoDB
        """;

    // 나보다 앞선 사람 수 + 1. 두 COUNT 모두 ix_score 범위 스캔이다.
    // (score, user_id) > (s, u) 같은 행 생성자 비교는 MySQL이 인덱스를 잘 못 써서 둘로 나눴다.
    private const string RankExpr = """
        1 + (SELECT COUNT(*) FROM ranking WHERE score > t.score)
          + (SELECT COUNT(*) FROM ranking WHERE score = t.score AND user_id > t.user_id)
        """;

    private const string RankByCountSql = "SELECT " + RankExpr + " FROM ranking t WHERE t.user_id = @u";

    private const string RankByWindowSql = """
        SELECT rnk FROM (
            SELECT user_id, ROW_NUMBER() OVER (ORDER BY score DESC, user_id DESC) AS rnk
            FROM ranking
        ) x
        WHERE x.user_id = @u
        """;

    public async Task ResetAsync()
    {
        await using var conn = await _dataSource.OpenConnectionAsync();
        await ExecuteAsync(conn, "DROP TABLE IF EXISTS ranking");
        await ExecuteAsync(conn, CreateTableSql);
    }

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
        await ExecuteAsync(conn, "ANALYZE TABLE ranking");
    }

    public async Task SetScoreAsync(int userId, long score)
    {
        await using var conn = await _dataSource.OpenConnectionAsync();
        await using var cmd = new MySqlCommand(
            "INSERT INTO ranking (user_id, score) VALUES (@u, @s) AS new ON DUPLICATE KEY UPDATE score = new.score",
            conn);
        cmd.Parameters.AddWithValue("@u", userId);
        cmd.Parameters.AddWithValue("@s", score);
        await cmd.ExecuteNonQueryAsync();
    }

    public async Task<long?> GetRankAsync(int userId)
    {
        await using var conn = await _dataSource.OpenConnectionAsync();
        await using var cmd = new MySqlCommand(_rankMode == MySqlRankMode.Count ? RankByCountSql : RankByWindowSql, conn);
        cmd.Parameters.AddWithValue("@u", userId);
        var result = await cmd.ExecuteScalarAsync();
        return result is null or DBNull ? null : Convert.ToInt64(result);
    }

    public async Task<IReadOnlyList<RankEntry>> GetTopAsync(int n)
    {
        await using var conn = await _dataSource.OpenConnectionAsync();
        await using var cmd = new MySqlCommand(
            "SELECT user_id, score FROM ranking ORDER BY score DESC, user_id DESC LIMIT @n", conn);
        cmd.Parameters.AddWithValue("@n", n);

        var result = new List<RankEntry>(n);
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
            result.Add(new RankEntry(reader.GetInt32(0), reader.GetInt64(1), result.Count + 1));
        return result;
    }

    // LIMIT ... OFFSET은 앞의 offset개를 인덱스에서 읽고 버린다. 비용은 offset에 비례.
    public async Task<IReadOnlyList<RankEntry>> GetPageAsync(long offset, int count)
    {
        await using var conn = await _dataSource.OpenConnectionAsync();
        await using var cmd = new MySqlCommand(
            "SELECT user_id, score FROM ranking ORDER BY score DESC, user_id DESC LIMIT @n OFFSET @o", conn);
        cmd.Parameters.AddWithValue("@n", count);
        cmd.Parameters.AddWithValue("@o", offset);

        var result = new List<RankEntry>(count);
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
            result.Add(new RankEntry(reader.GetInt32(0), reader.GetInt64(1), offset + result.Count + 1));
        return result;
    }

    public async Task<IReadOnlyList<RankEntry>> GetAroundAsync(int userId, int range)
    {
        await using var conn = await _dataSource.OpenConnectionAsync();

        // 1) 내 점수와 순위
        long myScore;
        long myRank;
        await using (var cmd = new MySqlCommand(
            "SELECT t.score, " + RankExpr + " FROM ranking t WHERE t.user_id = @u", conn))
        {
            cmd.Parameters.AddWithValue("@u", userId);
            await using var reader = await cmd.ExecuteReaderAsync();
            if (!await reader.ReadAsync())
                return [];
            myScore = reader.GetInt64(0);
            myRank = reader.GetInt64(1);
        }

        // 2) 내 위 range명(역순으로 읽어 뒤집는다) + 나 + 내 아래 range명
        await using (var cmd = new MySqlCommand("""
            (SELECT user_id, score, 0 AS part FROM ranking
              WHERE score > @s OR (score = @s AND user_id > @u)
              ORDER BY score ASC, user_id ASC LIMIT @k)
            UNION ALL
            (SELECT user_id, score, 1 AS part FROM ranking
              WHERE score < @s OR (score = @s AND user_id < @u)
              ORDER BY score DESC, user_id DESC LIMIT @k)
            """, conn))
        {
            cmd.Parameters.AddWithValue("@s", myScore);
            cmd.Parameters.AddWithValue("@u", userId);
            cmd.Parameters.AddWithValue("@k", range);

            var above = new List<(int, long)>(range);
            var below = new List<(int, long)>(range);
            await using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                var row = (reader.GetInt32(0), reader.GetInt64(1));
                if (reader.GetInt32(2) == 0)
                    above.Add(row);
                else
                    below.Add(row);
            }

            var result = new List<RankEntry>(above.Count + 1 + below.Count);
            for (var i = above.Count - 1; i >= 0; --i)
                result.Add(new RankEntry(above[i].Item1, above[i].Item2, myRank - (i + 1)));
            result.Add(new RankEntry(userId, myScore, myRank));
            for (var i = 0; i < below.Count; ++i)
                result.Add(new RankEntry(below[i].Item1, below[i].Item2, myRank + (i + 1)));
            return result;
        }
    }

    public async Task<long> CountAsync()
    {
        await using var conn = await _dataSource.OpenConnectionAsync();
        await using var cmd = new MySqlCommand("SELECT COUNT(*) FROM ranking", conn);
        return Convert.ToInt64(await cmd.ExecuteScalarAsync());
    }

    private static async Task ExecuteAsync(MySqlConnection conn, string sql)
    {
        await using var cmd = new MySqlCommand(sql, conn);
        await cmd.ExecuteNonQueryAsync();
    }

    public ValueTask DisposeAsync() => _dataSource.DisposeAsync();
}
