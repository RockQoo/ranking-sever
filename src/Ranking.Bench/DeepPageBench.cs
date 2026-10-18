using System.Diagnostics;
using System.Text;
using MySqlConnector;
using Ranking.Core;
using StackExchange.Redis;

/// <summary>
/// 깊은 페이지 조회(Deep Pagination): 랭킹 화면에서 offset 위치부터 10명을 닉네임과 함께 보여 주는 비용.
/// 닉네임은 일반적인 구조대로 별도 유저 테이블 users(user_id PK, nickname)에 있다.
///  - mysql-join:     sp_ranking_get_page_named. ranking JOIN users … LIMIT 10 OFFSET n (가장 흔하게 짜는 쿼리)
///  - mysql-deferred: sp_ranking_get_page_named_deferred. 지연 조인. 인덱스만으로 10명을 먼저 고르고 그 10명만 users와 JOIN
///  - mysql-snapshot: sp_snapshot_get_page_named. 배치로 미리 계산한 순위 테이블에서 rnk BETWEEN 범위 + users JOIN
///  - redis:          ZREVRANGE로 10명 + 닉네임 해시 HMGET (2회 왕복)
/// </summary>
static class DeepPageBench
{
    private const int PageSize = 10;
    private const string NickKey = "ranking:nick";
    private static readonly long[] Offsets = [500, 5_000, 10_000, 50_000, 100_000, 300_000, 500_000, 900_000];

    // EXPLAIN은 CALL에 쓸 수 없어서 sql/procedures.sql의 SP 본문과 같은 SELECT를 여기 둔다. SP를 고치면 같이 고친다.
    private const string JoinSql = """
        SELECT r.user_id, u.nickname, r.score
        FROM ranking r JOIN users u ON u.user_id = r.user_id
        ORDER BY r.score DESC, r.user_id DESC
        LIMIT 10 OFFSET @o
        """;

    private const string DeferredSql = """
        SELECT r.user_id, u.nickname, r.score
        FROM (SELECT user_id, score FROM ranking
              ORDER BY score DESC, user_id DESC
              LIMIT 10 OFFSET @o) r
        JOIN users u ON u.user_id = r.user_id
        ORDER BY r.score DESC, r.user_id DESC
        """;

    private const string SnapshotSql = """
        SELECT s.user_id, u.nickname, s.score
        FROM ranking_snapshot s JOIN users u ON u.user_id = s.user_id
        WHERE s.rnk BETWEEN @o + 1 AND @o + 10
        ORDER BY s.rnk
        """;

    private readonly record struct Row(int UserId, string Nickname, long Score);

    public static async Task RunAsync(BenchOptions options, Action<string> log)
    {
        var ops = options.PageOps;
        log("# 깊은 페이지 조회 — 닉네임 포함 랭킹 목록, Redis vs MySQL");
        log("");
        log($"- 일시: {DateTime.Now:yyyy-MM-dd HH:mm}");
        log($"- 인원: {options.Users:N0}명, 점수 0~{BenchOptions.MaxScore - 1:N0} 균등 난수(seed {BenchOptions.Seed})");
        log($"- 시나리오: offset 위치부터 {PageSize}명을 (user_id, 닉네임, 점수)로 조회. offset마다 {ops}회 평균(워밍업 3회 제외)");
        log("- 스키마: `ranking(user_id PK, score, KEY(score, user_id))` + `users(user_id PK, nickname VARCHAR(32))`");
        log("- mysql-join: `ranking JOIN users … ORDER BY score DESC, user_id DESC LIMIT 10 OFFSET n`");
        log("- mysql-deferred: 지연 조인. `(SELECT user_id, score FROM ranking ORDER BY … LIMIT 10 OFFSET n)`을 users와 JOIN");
        log("- mysql-snapshot: 순위 테이블 `rnk BETWEEN n+1 AND n+10` + users JOIN");
        log("- redis: `ZREVRANGE` + 닉네임 해시 `HMGET` (2회 왕복)");
        log("");

        var rng = new Random(BenchOptions.Seed);
        var rows = new (int UserId, long Score)[options.Users];
        for (var i = 0; i < rows.Length; ++i)
            rows[i] = (i + 1, rng.Next(BenchOptions.MaxScore));

        await using var redisStore = await RedisRankingStore.ConnectAsync(options.Redis);
        await using var mux = await ConnectionMultiplexer.ConnectAsync(options.Redis);
        var redisDb = mux.GetDatabase();
        await using var mysql = new MySqlRankingStore(options.MySql);
        await using var snapshot = new MySqlSnapshotRankingStore(options.MySql);
        await using var dataSource = new MySqlDataSource(options.MySql);

        // ---- 적재: 점수, 닉네임(users 테이블, Redis 해시), 순위 테이블
        var sw = Stopwatch.StartNew();
        await redisStore.ResetAsync();
        await redisStore.BulkLoadAsync(rows);
        await LoadRedisNicknamesAsync(redisDb, rows.Length);
        await mysql.ResetAsync();
        await mysql.BulkLoadAsync(rows);
        await LoadUsersAsync(dataSource, rows.Length);
        await snapshot.RebuildAsync();
        log($"> 적재 완료 ({sw.Elapsed.TotalSeconds:F1}s)");
        log("");

        Func<long, Task<List<Row>>>[] methods =
        [
            o => RedisPageAsync(redisStore, redisDb, o),
            o => CallPageAsync(dataSource, "CALL sp_ranking_get_page_named(@o, @n)", o),
            o => CallPageAsync(dataSource, "CALL sp_ranking_get_page_named_deferred(@o, @n)", o),
            o => CallPageAsync(dataSource, "CALL sp_snapshot_get_page_named(@o, @n)", o),
        ];
        var offsets = Offsets.Where(o => o + PageSize <= options.Users).ToArray();

        // ---- 네 방식 결과가 같은지 확인
        var mismatches = 0;
        foreach (var offset in offsets)
        {
            var expected = await methods[0](offset);
            if (expected.Count != PageSize)
                ++mismatches;
            foreach (var method in methods[1..])
                if (!(await method(offset)).SequenceEqual(expected))
                    ++mismatches;
        }
        log($"> 결과 일치 확인: offset {offsets.Length}곳에서 네 방식의 (user_id, 닉네임, 점수) 10명 비교 → 불일치 {mismatches}건");
        log("");
        if (mismatches > 0)
            throw new InvalidOperationException("방식끼리 결과가 다르다");

        // ---- 측정
        log("## 응답 시간 (평균 ms)");
        log("");
        log("| offset (순위) | redis | mysql-join | mysql-deferred | mysql-snapshot |");
        log("|---:|---:|---:|---:|---:|");
        foreach (var offset in offsets)
        {
            var avgs = new List<string>();
            foreach (var method in methods)
                avgs.Add($"{await MeasureAsync(() => method(offset), ops):F3}");
            log($"| {offset:N0} ({offset + 1:N0}~{offset + PageSize:N0}등) | {string.Join(" | ", avgs)} |");
        }
        log("");

        // ---- 실행 계획
        log("## 실행 계획");
        log("");
        log("| 쿼리 | offset | id | table | type | key | rows | Extra |");
        log("|---|---:|---:|---|---|---|---:|---|");
        foreach (var offset in new long[] { 500, 5_000, 900_000 }.Where(o => o + PageSize <= options.Users))
        {
            await ExplainAsync(dataSource, "mysql-join", JoinSql, offset, log);
            await ExplainAsync(dataSource, "mysql-deferred", DeferredSql, offset, log);
        }
        await ExplainAsync(dataSource, "mysql-snapshot", SnapshotSql, offsets[^1], log);
        log("");
    }

    private static async Task LoadRedisNicknamesAsync(IDatabase db, int users)
    {
        const int batchSize = 10_000;
        await db.KeyDeleteAsync(NickKey);
        for (var i = 1; i <= users; i += batchSize)
        {
            var count = Math.Min(batchSize, users - i + 1);
            var fields = new HashEntry[count];
            for (var j = 0; j < count; ++j)
                fields[j] = new HashEntry(i + j, Nickname(i + j));
            await db.HashSetAsync(NickKey, fields);
        }
    }

    private static async Task LoadUsersAsync(MySqlDataSource dataSource, int users)
    {
        const int batchSize = 5_000;
        await using var conn = await dataSource.OpenConnectionAsync();
        await ExecuteAsync(conn, "TRUNCATE TABLE users");   // 테이블은 sql/schema.sql

        await using var tx = await conn.BeginTransactionAsync();
        var sql = new StringBuilder();
        for (var i = 1; i <= users; i += batchSize)
        {
            var count = Math.Min(batchSize, users - i + 1);
            sql.Clear().Append("INSERT INTO users (user_id, nickname) VALUES ");
            for (var j = 0; j < count; ++j)
            {
                if (j > 0)
                    sql.Append(',');
                sql.Append('(').Append(i + j).Append(",'").Append(Nickname(i + j)).Append("')");
            }
            await using var cmd = new MySqlCommand(sql.ToString(), conn, tx);
            await cmd.ExecuteNonQueryAsync();
        }
        await tx.CommitAsync();
        await ExecuteAsync(conn, "ANALYZE TABLE users");
    }

    private static string Nickname(int userId) => $"player{userId}";

    private static async Task<List<Row>> RedisPageAsync(RedisRankingStore store, IDatabase db, long offset)
    {
        var page = await store.GetPageAsync(offset, PageSize);
        var names = await db.HashGetAsync(NickKey, page.Select(e => (RedisValue)e.UserId).ToArray());
        return page.Select((e, i) => new Row(e.UserId, names[i].ToString(), e.Score)).ToList();
    }

    private static async Task<List<Row>> CallPageAsync(MySqlDataSource dataSource, string call, long offset)
    {
        await using var conn = await dataSource.OpenConnectionAsync();
        await using var cmd = new MySqlCommand(call, conn);
        cmd.Parameters.AddWithValue("@o", offset);
        cmd.Parameters.AddWithValue("@n", PageSize);
        var result = new List<Row>(PageSize);
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
            result.Add(new Row(reader.GetInt32(0), reader.GetString(1), reader.GetInt64(2)));
        return result;
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

    private static async Task ExplainAsync(MySqlDataSource dataSource, string name, string sql, long offset, Action<string> log)
    {
        await using var conn = await dataSource.OpenConnectionAsync();
        await using var cmd = new MySqlCommand("EXPLAIN " + sql, conn);
        cmd.Parameters.AddWithValue("@o", offset);
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            string Col(string c) => reader[c] is DBNull ? "" : reader[c].ToString() ?? "";
            log($"| {name} | {offset:N0} | {Col("id")} | {Col("table")} | {Col("type")} | {Col("key")} | {Col("rows")} | {Col("Extra")} |");
        }
    }

    private static async Task ExecuteAsync(MySqlConnection conn, string sql)
    {
        await using var cmd = new MySqlCommand(sql, conn);
        await cmd.ExecuteNonQueryAsync();
    }
}
