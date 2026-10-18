using System.Diagnostics;
using System.Text;
using MySqlConnector;
using Ranking.Core;

/// <summary>
/// 깊은 페이지 조회(Deep Pagination): offset 위치부터 10명을 읽는 비용이 offset에 따라 어떻게 변하는지 잰다.
/// MySQL은 두 가지로 잰다.
///  - mysql: 이 프로젝트 스키마. (score, user_id) 커버링 인덱스, 동점 정렬까지 Redis와 같음
///  - mysql-score-only: user_id, username, score + score 단일 인덱스, SELECT에 username 포함 (커버링 아님)
/// </summary>
static class DeepPageBench
{
    private const int PageSize = 10;
    private static readonly long[] Offsets = [500, 5_000, 10_000, 50_000, 100_000, 300_000, 500_000, 900_000];

    private const string ScoreOnlyTableSql = """
        CREATE TABLE ranking_score_only (
            user_id  INT         NOT NULL PRIMARY KEY,
            username VARCHAR(32) NOT NULL,
            score    BIGINT      NOT NULL,
            KEY ix_score (score)
        ) ENGINE=InnoDB
        """;

    private const string ScoreOnlyPageSql =
        "SELECT user_id, username, score FROM ranking_score_only ORDER BY score DESC LIMIT 10 OFFSET @o";

    public static async Task RunAsync(BenchOptions options, Action<string> log)
    {
        var ops = options.PageOps;
        log("# 깊은 페이지 조회 — Redis ZREVRANGE vs MySQL LIMIT OFFSET");
        log("");
        log($"- 일시: {DateTime.Now:yyyy-MM-dd HH:mm}");
        log($"- 인원: {options.Users:N0}명, 점수 0~{BenchOptions.MaxScore - 1:N0} 균등 난수(seed {BenchOptions.Seed})");
        log($"- 시나리오: offset 위치부터 {PageSize}명 조회. offset마다 {ops}회 측정(워밍업 3회 제외)");
        log("- mysql: (score, user_id) 커버링 인덱스, `ORDER BY score DESC, user_id DESC LIMIT 10 OFFSET n`");
        log("- mysql-score-only: (user_id, username, score) + `KEY(score)`, `SELECT user_id, username, score … ORDER BY score DESC LIMIT 10 OFFSET n`");
        log("");

        var rng = new Random(BenchOptions.Seed);
        var rows = new (int UserId, long Score)[options.Users];
        for (var i = 0; i < rows.Length; ++i)
            rows[i] = (i + 1, rng.Next(BenchOptions.MaxScore));

        await using var redis = await RedisRankingStore.ConnectAsync(options.Redis);
        await using var mysql = new MySqlRankingStore(options.MySql);
        await using var dataSource = new MySqlDataSource(options.MySql);

        // ---- 적재
        foreach (IRankingStore store in new IRankingStore[] { redis, mysql })
        {
            await store.ResetAsync();
            await store.BulkLoadAsync(rows);
        }
        var sw = Stopwatch.StartNew();
        await LoadScoreOnlyAsync(dataSource, rows);
        log($"> 적재 완료 (mysql-score-only {sw.Elapsed.TotalSeconds:F1}s)");
        log("");

        // ---- Redis와 mysql(커버링) 결과가 같은지 확인
        var mismatches = 0;
        foreach (var offset in Offsets.Where(o => o < options.Users))
            if (!(await redis.GetPageAsync(offset, PageSize)).SequenceEqual(await mysql.GetPageAsync(offset, PageSize)))
                ++mismatches;
        log($"> 결과 일치 확인: offset {Offsets.Length}곳에서 Redis vs mysql → 불일치 {mismatches}건 (score-only는 동점 순서가 정해지지 않아 비교 제외)");
        log("");

        // ---- 측정
        log("| offset (순위) | Redis 평균 | mysql 평균 | mysql-score-only 평균 | mysql / Redis | score-only / Redis |");
        log("|---:|---:|---:|---:|---:|---:|");
        foreach (var offset in Offsets.Where(o => o < options.Users))
        {
            var r = await MeasureAsync(() => redis.GetPageAsync(offset, PageSize), ops);
            var m = await MeasureAsync(() => mysql.GetPageAsync(offset, PageSize), ops);
            var s = await MeasureAsync(() => ScoreOnlyPageAsync(dataSource, offset), ops);
            log($"| {offset:N0} ({offset + 1:N0}~{offset + PageSize:N0}등) | {r:F3} | {m:F3} | {s:F3} | {m / r:F0}배 | {s / r:F0}배 |");
        }
        log("");

        // ---- 실행 계획
        log("## 실행 계획 (offset 900,000)");
        log("");
        log("| 쿼리 | type | key | rows | Extra |");
        log("|---|---|---|---:|---|");
        await ExplainAsync(dataSource, "mysql",
            "SELECT user_id, score FROM ranking ORDER BY score DESC, user_id DESC LIMIT 10 OFFSET 900000", log);
        await ExplainAsync(dataSource, "mysql-score-only", ScoreOnlyPageSql.Replace("@o", "900000"), log);
        log("");
    }

    private static async Task LoadScoreOnlyAsync(MySqlDataSource dataSource, (int UserId, long Score)[] rows)
    {
        const int batchSize = 5_000;
        await using var conn = await dataSource.OpenConnectionAsync();
        await ExecuteAsync(conn, "DROP TABLE IF EXISTS ranking_score_only");
        await ExecuteAsync(conn, ScoreOnlyTableSql);

        await using var tx = await conn.BeginTransactionAsync();
        var sql = new StringBuilder();
        for (var i = 0; i < rows.Length; i += batchSize)
        {
            var count = Math.Min(batchSize, rows.Length - i);
            sql.Clear().Append("INSERT INTO ranking_score_only (user_id, username, score) VALUES ");
            for (var j = 0; j < count; ++j)
            {
                var (userId, score) = rows[i + j];
                if (j > 0)
                    sql.Append(',');
                sql.Append('(').Append(userId).Append(",'user").Append(userId).Append("',").Append(score).Append(')');
            }
            await using var cmd = new MySqlCommand(sql.ToString(), conn, tx);
            await cmd.ExecuteNonQueryAsync();
        }
        await tx.CommitAsync();
        await ExecuteAsync(conn, "ANALYZE TABLE ranking_score_only");
    }

    private static async Task<int> ScoreOnlyPageAsync(MySqlDataSource dataSource, long offset)
    {
        await using var conn = await dataSource.OpenConnectionAsync();
        await using var cmd = new MySqlCommand(ScoreOnlyPageSql, conn);
        cmd.Parameters.AddWithValue("@o", offset);
        var count = 0;
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
            ++count;
        return count;
    }

    private static async Task<double> MeasureAsync(Func<Task> action, int ops)
    {
        for (var i = 0; i < 3; ++i)
            await action();

        var total = 0.0;
        for (var i = 0; i < ops; ++i)
        {
            var start = Stopwatch.GetTimestamp();
            await action();
            total += Stopwatch.GetElapsedTime(start).TotalMilliseconds;
        }
        return total / ops;
    }

    private static async Task ExplainAsync(MySqlDataSource dataSource, string name, string sql, Action<string> log)
    {
        await using var conn = await dataSource.OpenConnectionAsync();
        await using var cmd = new MySqlCommand("EXPLAIN " + sql, conn);
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            string Col(string c) => reader[c] is DBNull ? "" : reader[c].ToString() ?? "";
            log($"| {name} | {Col("type")} | {Col("key")} | {Col("rows")} | {Col("Extra")} |");
        }
    }

    private static async Task ExecuteAsync(MySqlConnection conn, string sql)
    {
        await using var cmd = new MySqlCommand(sql, conn);
        await cmd.ExecuteNonQueryAsync();
    }
}
