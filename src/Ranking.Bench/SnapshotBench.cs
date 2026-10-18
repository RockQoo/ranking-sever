using System.Collections.Concurrent;
using System.Diagnostics;
using MySqlConnector;
using Ranking.Core;

/// <summary>
/// 순위 테이블 배치 재계산 + 바꿔치기. 점수는 원본 ranking에 쌓고, 주기적으로 ranking_snapshot에 순위를 계산해 넣는다.
///  1. 조회 속도: Redis / mysql(실시간 COUNT) / mysql-snapshot(미리 계산한 rnk)
///  2. 재계산 방식별 소요 시간, 재계산 중 게임 조회가 어떻게 되는지
///  3. 재계산 중 원본 점수 갱신이 막히는지 (격리 수준별)
/// </summary>
static class SnapshotBench
{
    private const int LoadWorkers = 8;
    private const int ThroughputOps = 3_000;

    public static async Task RunAsync(BenchOptions options, Action<string> log)
    {
        var ops = options.SnapshotOps;
        log("# 순위 테이블 배치 재계산 + 바꿔치기 — MySQL");
        log("");
        log($"- 일시: {DateTime.Now:yyyy-MM-dd HH:mm}");
        log($"- 인원: {options.Users:N0}명, 점수 0~{BenchOptions.MaxScore - 1:N0} 균등 난수(seed {BenchOptions.Seed})");
        log("- 원본 `ranking(user_id PK, score, KEY(score, user_id))` → 순위 테이블 `ranking_snapshot(user_id PK, score, rnk UNIQUE)`");
        log("- 재계산: `INSERT INTO … SELECT user_id, score, ROW_NUMBER() OVER (ORDER BY score DESC, user_id DESC) FROM ranking`");
        log("");

        var rng = new Random(BenchOptions.Seed);
        var rows = new (int UserId, long Score)[options.Users];
        for (var i = 0; i < rows.Length; ++i)
            rows[i] = (i + 1, rng.Next(BenchOptions.MaxScore));
        var bottomUsers = rows.OrderBy(r => r.Score).ThenBy(r => r.UserId).Take(Math.Max(1, rows.Length / 100)).Select(r => r.UserId).ToArray();
        int RandomUser() => Random.Shared.Next(1, options.Users + 1);

        await using var redis = await RedisRankingStore.ConnectAsync(options.Redis);
        await using var live = new MySqlRankingStore(options.MySql);
        await using var snapshot = new MySqlSnapshotRankingStore(options.MySql);
        IRankingStore[] stores = [redis, live, snapshot];

        // ---- 적재와 첫 재계산
        await redis.ResetAsync();
        await redis.BulkLoadAsync(rows);
        await snapshot.ResetAsync();          // 원본 ranking도 같이 비운다
        await snapshot.BulkLoadAsync(rows);   // 원본 ranking에 넣는다
        var sw = Stopwatch.StartNew();
        await snapshot.RebuildAsync();
        log($"> 적재 완료. 첫 재계산 {sw.Elapsed.TotalSeconds:F1}s");
        log("");

        // ---- 결과 일치 확인
        var mismatches = 0;
        var vr = new Random(7);
        for (var i = 0; i < 200; ++i)
        {
            var u = vr.Next(1, options.Users + 1);
            if (await redis.GetRankAsync(u) != await snapshot.GetRankAsync(u))
                ++mismatches;
        }
        if (!(await redis.GetTopAsync(100)).SequenceEqual(await snapshot.GetTopAsync(100)))
            ++mismatches;
        for (var i = 0; i < 20; ++i)
        {
            var u = vr.Next(1, options.Users + 1);
            if (!(await redis.GetAroundAsync(u, 5)).SequenceEqual(await snapshot.GetAroundAsync(u, 5)))
                ++mismatches;
        }
        var deepOffset = options.Users * 9L / 10;
        if (!(await redis.GetPageAsync(deepOffset, 10)).SequenceEqual(await snapshot.GetPageAsync(deepOffset, 10)))
            ++mismatches;
        log($"> 결과 일치 확인: Redis vs mysql-snapshot — 순위 200건 · 상위 100명 · 주변 ±5명 20건 · 깊은 페이지 1건 → 불일치 {mismatches}건");
        log("");
        if (mismatches > 0)
            throw new InvalidOperationException("Redis와 mysql-snapshot 결과가 다르다");

        // ---- 1. 순차 조회
        log($"## 1. 순차 조회 (클라이언트 1개, {ops}회 평균, 단위 ms)");
        log("");
        log("| 연산 | redis | mysql (실시간 COUNT) | mysql-snapshot |");
        log("|---|---:|---:|---:|");
        await RowAsync("내 순위 (무작위 유저)", s => s.GetRankAsync(RandomUser()));
        await RowAsync("내 순위 (하위 1% 유저)", s => s.GetRankAsync(bottomUsers[Random.Shared.Next(bottomUsers.Length)]));
        await RowAsync("상위 100명", s => s.GetTopAsync(100));
        await RowAsync("내 주변 ±5명", s => s.GetAroundAsync(RandomUser(), 5));
        await RowAsync($"깊은 페이지 ({deepOffset:N0}등부터 10명)", s => s.GetPageAsync(deepOffset, 10));
        log("");

        // 새로 채운 테이블은 통계가 비어 있다. ANALYZE 없이 바꿔치기하면 옵티마이저가 rnk 인덱스를 안 쓴다.
        // InnoDB는 행이 10% 넘게 바뀌면 백그라운드에서 통계를 다시 계산하므로, 그 전까지만 계획이 틀린다.
        const string topSql = "SELECT user_id, score, rnk FROM ranking_snapshot WHERE rnk BETWEEN 1 AND 100 ORDER BY rnk";
        await snapshot.RebuildAsync(analyze: false);
        var planRight = await ExplainAsync(options.MySql, topSql);
        var firstTop = await AverageAsync(() => snapshot.GetTopAsync(100), 1, warmup: 0);
        var noAnalyzeTop = await AverageAsync(() => snapshot.GetTopAsync(100), ops);
        var planAfter = await ExplainAsync(options.MySql, topSql);
        await snapshot.RebuildAsync();
        log("> 재계산 후 `ANALYZE TABLE`을 안 하면 (새 테이블 통계가 \"빈 테이블\"):");
        log($"> - 바꿔치기 직후 계획 `{planRight}`, 첫 조회 {firstTop:F3}ms");
        log($"> - 상위 100명 {ops}회 평균 {noAnalyzeTop:F3}ms, 측정 후 계획 `{planAfter}` (백그라운드 통계 자동 재계산 여부에 따라 바뀜)");
        log("");

        async Task RowAsync(string op, Func<IRankingStore, Task> action)
        {
            var avgs = new List<string>();
            foreach (var store in stores)
                avgs.Add($"{await AverageAsync(() => action(store), ops):F3}");
            log($"| {op} | {string.Join(" | ", avgs)} |");
        }

        // ---- 2. 동시 처리량
        log($"## 2. 동시 처리량 — 내 순위 (무작위 유저), {options.Concurrency}개 작업 × 합계 {ThroughputOps:N0}회");
        log("");
        log("| 저장소 | 초당 처리 | p50 ms | p99 ms |");
        log("|---|---:|---:|---:|");
        foreach (var store in stores)
        {
            var (perSec, st) = await ThroughputAsync(() => store.GetRankAsync(RandomUser()), options.Concurrency, ThroughputOps);
            log($"| {store.Name} | {perSec:N0} | {st.P50:F3} | {st.P99:F3} |");
        }
        log("");

        // ---- 3. 재계산 방식별 소요 시간 (부하 없음)
        log("## 3. 재계산 소요 시간 (부하 없음)");
        log("");
        log("| 방식 | 소요 |");
        log("|---|---:|");
        foreach (var mode in Enum.GetValues<SnapshotRebuildMode>())
        {
            sw.Restart();
            await snapshot.RebuildAsync(mode);
            log($"| {ModeName(mode)} | {sw.Elapsed.TotalSeconds:F2}s |");
        }
        log("");

        // ---- 4. 재계산 중 게임 조회
        log($"## 4. 재계산 중 게임 조회 — 순위 테이블에서 내 순위 조회 {LoadWorkers}개 작업이 계속 도는 동안 재계산");
        log("");
        log("| 방식 | 재계산 | 조회 수 | 못 찾음 | 오류 | p50 ms | p99 ms | 최대 ms |");
        log("|---|---:|---:|---:|---:|---:|---:|---:|");
        var errorNotes = new List<string>();
        foreach (var mode in Enum.GetValues<SnapshotRebuildMode>())
        {
            var r = await UnderLoadAsync(() => snapshot.RebuildAsync(mode),
                async () => await snapshot.GetRankAsync(RandomUser()) is not null);
            log(r.Row(ModeName(mode)));
            errorNotes.AddRange(r.ErrorNotes(ModeName(mode)));
        }

        // 바꿔치기 직전에 순위 테이블을 읽은 트랜잭션이 열려 있으면 RENAME이 메타데이터 잠금(MDL)을 기다리고,
        // 그 뒤에 오는 조회도 RENAME 뒤에 줄을 선다. RENAME이 잠금을 기다리기 시작한 뒤 2초 더 열어 둔다.
        var hold = TimeSpan.FromSeconds(2);
        var holdResult = await UnderLoadAsync(
            async () =>
            {
                var holder = HoldReadTransactionAsync(options.MySql, hold);
                await snapshot.RebuildAsync(SnapshotRebuildMode.Swap);
                await holder;
            },
            async () => await snapshot.GetRankAsync(RandomUser()) is not null);
        log(holdResult.Row($"바꿔치기 + 순위 테이블을 읽은 트랜잭션이 RENAME을 {hold.TotalSeconds:F0}s 막음"));
        errorNotes.AddRange(holdResult.ErrorNotes("트랜잭션 열려 있음"));
        log("");
        LogErrorNotes();

        void LogErrorNotes()
        {
            foreach (var note in errorNotes)
                log($"> 오류 — {note}");
            if (errorNotes.Count > 0)
                log("");
            errorNotes.Clear();
        }

        // ---- 5. 재계산 중 점수 갱신
        log($"## 5. 재계산 중 점수 갱신 — 원본 ranking에 점수 갱신 {LoadWorkers}개 작업이 계속 도는 동안 바꿔치기 재계산");
        log("");
        log("| 재계산 세션 격리 수준 | 재계산 | 갱신 수 | 못 찾음 | 오류 | p50 ms | p99 ms | 최대 ms |");
        log("|---|---:|---:|---:|---:|---:|---:|---:|");
        foreach (var readCommitted in new[] { false, true })
        {
            var r = await UnderLoadAsync(() => snapshot.RebuildAsync(SnapshotRebuildMode.Swap, readCommitted),
                async () => { await live.SetScoreAsync(RandomUser(), Random.Shared.Next(BenchOptions.MaxScore)); return true; });
            var name = readCommitted ? "READ COMMITTED" : "REPEATABLE READ (기본)";
            log(r.Row(name));
            errorNotes.AddRange(r.ErrorNotes(name));
        }
        log("");
        LogErrorNotes();
    }

    private static async Task<string> ExplainAsync(string connectionString, string sql)
    {
        await using var conn = new MySqlConnection(connectionString);
        await conn.OpenAsync();
        await using var cmd = new MySqlCommand("EXPLAIN " + sql, conn);
        await using var reader = await cmd.ExecuteReaderAsync();
        await reader.ReadAsync();
        string Col(string c) => reader[c] is DBNull ? "NULL" : reader[c].ToString() ?? "";
        return $"type={Col("type")}, key={Col("key")}, rows={Col("rows")}, Extra={Col("Extra")}";
    }

    private static string ModeName(SnapshotRebuildMode mode) => mode switch
    {
        SnapshotRebuildMode.Swap => "새 테이블 + RENAME 바꿔치기",
        SnapshotRebuildMode.Truncate => "TRUNCATE 후 다시 채우기",
        SnapshotRebuildMode.DeleteInTransaction => "한 트랜잭션에서 DELETE 후 다시 채우기",
        _ => mode.ToString(),
    };

    private sealed record LoadResult(double BatchSeconds, int Count, int NotFound, int Errors, Stats Stats, IReadOnlyDictionary<string, int> ErrorMessages)
    {
        public string Row(string name) =>
            $"| {name} | {BatchSeconds:F2}s | {Count:N0} | {NotFound:N0} | {Errors:N0} | {Stats.P50:F3} | {Stats.P99:F3} | {Stats.Max:F1} |";

        public IEnumerable<string> ErrorNotes(string name) =>
            ErrorMessages.Select(e => $"{name}: {e.Value}건 `{e.Key}`");
    }

    /// <summary>op를 LoadWorkers개 작업으로 계속 돌리면서 batch를 실행한다. batch 앞뒤로 0.5초씩 여유를 둔다.</summary>
    private static async Task<LoadResult> UnderLoadAsync(Func<Task> batch, Func<Task<bool>> op)
    {
        var samples = new ConcurrentBag<double>();
        var notFound = 0;
        var errors = 0;
        var errorMessages = new ConcurrentDictionary<string, int>();
        using var cts = new CancellationTokenSource();
        var workers = Enumerable.Range(0, LoadWorkers).Select(_ => Task.Run(async () =>
        {
            while (!cts.IsCancellationRequested)
            {
                var start = Stopwatch.GetTimestamp();
                try
                {
                    if (!await op())
                        Interlocked.Increment(ref notFound);
                }
                catch (MySqlException e)
                {
                    Interlocked.Increment(ref errors);
                    errorMessages.AddOrUpdate(e.Message, 1, (_, n) => n + 1);
                }
                samples.Add(Stopwatch.GetElapsedTime(start).TotalMilliseconds);
            }
        })).ToArray();

        await Task.Delay(500);
        var sw = Stopwatch.StartNew();
        await batch();
        sw.Stop();
        await Task.Delay(500);
        cts.Cancel();
        await Task.WhenAll(workers);
        return new LoadResult(sw.Elapsed.TotalSeconds, samples.Count, notFound, errors, Stats.From(samples.ToArray()), errorMessages);
    }

    /// <summary>
    /// 순위 테이블을 한 번 읽고 트랜잭션을 열어 둔다 (긴 조회·정산 쿼리가 걸려 있는 상황).
    /// RENAME이 메타데이터 잠금을 기다리기 시작하면 hold만큼 더 있다가 커밋한다.
    /// </summary>
    private static async Task HoldReadTransactionAsync(string connectionString, TimeSpan hold)
    {
        await using var conn = new MySqlConnection(connectionString);
        await conn.OpenAsync();
        await using var tx = await conn.BeginTransactionAsync();
        await using (var cmd = new MySqlCommand("SELECT rnk FROM ranking_snapshot WHERE user_id = 1", conn, tx))
            await cmd.ExecuteScalarAsync();

        await using var watch = new MySqlConnection(connectionString);
        await watch.OpenAsync();
        var deadline = Stopwatch.StartNew();
        while (deadline.Elapsed < TimeSpan.FromSeconds(120))
        {
            await using var cmd = new MySqlCommand(
                "SELECT COUNT(*) FROM information_schema.PROCESSLIST WHERE STATE = 'Waiting for table metadata lock' AND INFO LIKE 'RENAME TABLE%'",
                watch);
            if (Convert.ToInt64(await cmd.ExecuteScalarAsync()) > 0)
                break;
            await Task.Delay(20);
        }
        await Task.Delay(hold);
        await tx.CommitAsync();
    }

    private static async Task<double> AverageAsync(Func<Task> action, int ops, int warmup = 10)
    {
        for (var i = 0; i < warmup; ++i)
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

    private static async Task<(double PerSecond, Stats Stats)> ThroughputAsync(Func<Task> action, int concurrency, int total)
    {
        var samples = new double[total];
        var next = -1;
        var sw = Stopwatch.StartNew();
        await Task.WhenAll(Enumerable.Range(0, concurrency).Select(async _ =>
        {
            int i;
            while ((i = Interlocked.Increment(ref next)) < samples.Length)
            {
                var start = Stopwatch.GetTimestamp();
                await action();
                samples[i] = Stopwatch.GetElapsedTime(start).TotalMilliseconds;
            }
        }));
        sw.Stop();
        return (total / sw.Elapsed.TotalSeconds, Stats.From(samples));
    }
}
