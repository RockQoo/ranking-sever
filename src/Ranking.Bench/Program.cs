using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Text;
using Ranking.Core;

// 사용법: dotnet run -c Release --project src/Ranking.Bench -- [--users 100000] [--ops 2000] [--concurrency 32] [--throughput-ops 20000]
//         깊은 페이지 조회만: ... -- --deep-page [--users 1000000] [--page-ops 30]
//         순위 테이블 배치 재계산: ... -- --snapshot [--users 1000000] [--snapshot-ops 200]
var options = BenchOptions.Parse(args);
var report = new StringBuilder();

void Log(string line = "")
{
    Console.WriteLine(line);
    report.AppendLine(line);
}

if (args.Contains("--deep-page"))
{
    await DeepPageBench.RunAsync(options, line => Log(line));
    await SaveReportAsync($"users{options.Users}-deep-page");
    return;
}

if (args.Contains("--snapshot"))
{
    await SnapshotBench.RunAsync(options, line => Log(line));
    await SaveReportAsync($"users{options.Users}-snapshot");
    return;
}

Log($"# 랭킹 벤치마크 — Redis ZSET vs MySQL");
Log();
Log($"- 일시: {DateTime.Now:yyyy-MM-dd HH:mm}");
Log($"- 인원: {options.Users:N0}명, 점수 0~{BenchOptions.MaxScore - 1:N0} 균등 난수(seed {BenchOptions.Seed})");
Log($"- 순차 측정: 연산당 {options.Ops:N0}회 (워밍업 {options.Warmup}회 제외), 동시 측정: {options.Concurrency}개 작업 × 합계 {options.ThroughputOps:N0}회");
Log($"- 클라이언트: {RuntimeInformation.OSDescription}, .NET {Environment.Version}");
Log($"- 서버: WSL2 Ubuntu (Redis·MySQL 기본 설정), 같은 PC의 localhost 접속");
Log();

// ---- 데이터 생성: 두 저장소에 같은 데이터를 넣는다
var rng = new Random(BenchOptions.Seed);
var rows = new (int UserId, long Score)[options.Users];
for (var i = 0; i < rows.Length; ++i)
    rows[i] = (i + 1, rng.Next(BenchOptions.MaxScore));

// 정렬 기준(점수 내림차순, 동점이면 user_id 내림차순)으로 상위·하위 1% 유저를 미리 뽑는다
var sorted = rows.OrderByDescending(r => r.Score).ThenByDescending(r => r.UserId).Select(r => r.UserId).ToArray();
var onePercent = Math.Max(1, sorted.Length / 100);
var topUsers = sorted[..onePercent];
var bottomUsers = sorted[^onePercent..];

await using var redis = await RedisRankingStore.ConnectAsync(options.Redis);
await using var mysql = new MySqlRankingStore(options.MySql);
await using var mysqlWindow = new MySqlRankingStore(options.MySql, MySqlRankMode.Window);
IRankingStore[] stores = [redis, mysql];

// ---- 1. 초기 적재
Log("## 1. 초기 적재");
Log();
Log("| 저장소 | 시간 | 초당 행 |");
Log("|---|---:|---:|");
foreach (var store in stores)
{
    await store.ResetAsync();
    var sw = Stopwatch.StartNew();
    await store.BulkLoadAsync(rows);
    sw.Stop();
    var count = await store.CountAsync();
    if (count != rows.Length)
        throw new InvalidOperationException($"{store.Name}: {count} != {rows.Length}");
    Log($"| {store.Name} | {sw.Elapsed.TotalSeconds:F2}s | {rows.Length / sw.Elapsed.TotalSeconds:N0} |");
}
Log();

// ---- 2. 두 저장소의 결과가 같은지 확인
await VerifySameAsync("적재 직후");

// ---- 3. 순차 지연 시간 (클라이언트 1개, 요청 하나 끝나면 다음 요청)
Log("## 2. 순차 지연 시간 (클라이언트 1개, 단위 ms)");
Log();
Log("| 연산 | 저장소 | 평균 | p50 | p95 | p99 | 최대 |");
Log("|---|---|---:|---:|---:|---:|---:|");

await MeasureAllAsync("내 순위 (무작위 유저)", s => s.GetRankAsync(RandomUser()));
await MeasureAllAsync("내 순위 (상위 1% 유저)", s => s.GetRankAsync(topUsers[Random.Shared.Next(topUsers.Length)]));
await MeasureAllAsync("내 순위 (하위 1% 유저)", s => s.GetRankAsync(bottomUsers[Random.Shared.Next(bottomUsers.Length)]));
await MeasureAsync("내 순위 (무작위 유저)", mysqlWindow, s => s.GetRankAsync(RandomUser()), Math.Min(options.Ops, options.WindowOps), warmup: 2);
await MeasureAllAsync("상위 100명", s => s.GetTopAsync(100));
await MeasureAllAsync("내 주변 ±5명", s => s.GetAroundAsync(RandomUser(), 5));

// 점수 갱신은 두 저장소에 같은 순서로 같은 값을 넣어야 끝나고 다시 비교할 수 있다
var updates = MakeUpdates(options.Warmup + options.Ops, seed: 1);
foreach (var store in stores)
{
    var i = 0;
    await MeasureAsync("점수 갱신", store, s => { var (u, sc) = updates[i++]; return s.SetScoreAsync(u, sc); }, options.Ops, options.Warmup);
}
Log();

// ---- 4. 동시 처리량 (여러 클라이언트가 동시에 요청)
Log($"## 3. 동시 처리량 ({options.Concurrency}개 작업 동시, 단위 ms)");
Log();
Log("| 연산 | 저장소 | 초당 처리 | 평균 | p50 | p99 |");
Log("|---|---|---:|---:|---:|---:|");
var concurrentUpdates = MakeUpdates(options.ThroughputOps, seed: 2);
foreach (var store in stores)
    await ThroughputAsync("점수 갱신", store, i => { var (u, sc) = concurrentUpdates[i]; return store.SetScoreAsync(u, sc); });
foreach (var store in stores)
    await ThroughputAsync("내 순위 (무작위 유저)", store, _ => store.GetRankAsync(RandomUser()));
foreach (var store in stores)
    await ThroughputAsync("상위 100명", store, _ => store.GetTopAsync(100));
Log();

// 동시 갱신은 같은 유저가 여러 번 나오면 적용 순서가 저장소마다 달라질 수 있어서,
// 비교 전에 마지막 값을 한 번 더 순서대로 덮어써 맞춘다
var lastScore = new Dictionary<int, long>();
foreach (var (u, sc) in concurrentUpdates)
    lastScore[u] = sc;
foreach (var store in stores)
    foreach (var (u, sc) in lastScore)
        await store.SetScoreAsync(u, sc);
await VerifySameAsync("갱신 이후");

await SaveReportAsync($"users{options.Users}");
return;

// ----------------------------------------------------------------------------

async Task SaveReportAsync(string suffix)
{
    var dir = Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "..", "..", "results");
    Directory.CreateDirectory(dir);
    var file = Path.GetFullPath(Path.Combine(dir, $"{DateTime.Now:yyyyMMdd-HHmm}-{suffix}.md"));
    await File.WriteAllTextAsync(file, report.ToString());
    Console.WriteLine($"결과 저장: {file}");
}

int RandomUser() => Random.Shared.Next(1, options.Users + 1);

(int UserId, long Score)[] MakeUpdates(int count, int seed)
{
    var r = new Random(seed);
    var result = new (int, long)[count];
    for (var i = 0; i < count; ++i)
        result[i] = (r.Next(1, options.Users + 1), r.Next(BenchOptions.MaxScore));
    return result;
}

async Task MeasureAllAsync(string op, Func<IRankingStore, Task> action)
{
    foreach (var store in stores)
        await MeasureAsync(op, store, action, options.Ops, options.Warmup);
}

async Task MeasureAsync(string op, IRankingStore store, Func<IRankingStore, Task> action, int ops, int warmup)
{
    for (var i = 0; i < warmup; ++i)
        await action(store);

    var samples = new double[ops];
    for (var i = 0; i < ops; ++i)
    {
        var start = Stopwatch.GetTimestamp();
        await action(store);
        samples[i] = Stopwatch.GetElapsedTime(start).TotalMilliseconds;
    }
    var st = Stats.From(samples);
    Log($"| {op} | {store.Name}{(ops != options.Ops ? $" ({ops}회)" : "")} | {st.Avg:F3} | {st.P50:F3} | {st.P95:F3} | {st.P99:F3} | {st.Max:F3} |");
}

async Task ThroughputAsync(string op, IRankingStore store, Func<int, Task> action)
{
    var samples = new double[options.ThroughputOps];
    var next = -1;
    var sw = Stopwatch.StartNew();
    var workers = Enumerable.Range(0, options.Concurrency).Select(async _ =>
    {
        int i;
        while ((i = Interlocked.Increment(ref next)) < samples.Length)
        {
            var start = Stopwatch.GetTimestamp();
            await action(i);
            samples[i] = Stopwatch.GetElapsedTime(start).TotalMilliseconds;
        }
    });
    await Task.WhenAll(workers);
    sw.Stop();
    var st = Stats.From(samples);
    Log($"| {op} | {store.Name} | {samples.Length / sw.Elapsed.TotalSeconds:N0} | {st.Avg:F3} | {st.P50:F3} | {st.P99:F3} |");
}

async Task VerifySameAsync(string when)
{
    var mismatches = 0;
    var r = new Random(7);
    for (var i = 0; i < 200; ++i)
    {
        var u = r.Next(1, options.Users + 1);
        if (await redis.GetRankAsync(u) != await mysql.GetRankAsync(u))
            ++mismatches;
    }
    for (var i = 0; i < 3; ++i)
    {
        var u = r.Next(1, options.Users + 1);
        if (await mysqlWindow.GetRankAsync(u) != await redis.GetRankAsync(u))
            ++mismatches;
    }
    if (!(await redis.GetTopAsync(100)).SequenceEqual(await mysql.GetTopAsync(100)))
        ++mismatches;
    for (var i = 0; i < 20; ++i)
    {
        var u = r.Next(1, options.Users + 1);
        if (!(await redis.GetAroundAsync(u, 5)).SequenceEqual(await mysql.GetAroundAsync(u, 5)))
            ++mismatches;
    }
    Log($"> 결과 일치 확인({when}): 순위 203건 · 상위 100명 · 주변 ±5명 20건 비교 → 불일치 {mismatches}건");
    Log();
    if (mismatches > 0)
        throw new InvalidOperationException("Redis와 MySQL 결과가 다르다");
}

sealed record BenchOptions(int Users, int Ops, int Warmup, int Concurrency, int ThroughputOps, int WindowOps, int PageOps, int SnapshotOps, string Redis, string MySql)
{
    public const int Seed = 42;
    public const int MaxScore = 1_000_000;

    public static BenchOptions Parse(string[] args)
    {
        string Get(string name, string fallback)
        {
            var i = Array.IndexOf(args, name);
            return i >= 0 && i + 1 < args.Length ? args[i + 1] : fallback;
        }

        return new BenchOptions(
            Users: int.Parse(Get("--users", "100000")),
            Ops: int.Parse(Get("--ops", "2000")),
            Warmup: int.Parse(Get("--warmup", "200")),
            Concurrency: int.Parse(Get("--concurrency", "32")),
            ThroughputOps: int.Parse(Get("--throughput-ops", "20000")),
            WindowOps: int.Parse(Get("--window-ops", "20")),
            PageOps: int.Parse(Get("--page-ops", "30")),
            SnapshotOps: int.Parse(Get("--snapshot-ops", "200")),
            Redis: Get("--redis", "127.0.0.1:6379"),
            MySql: Get("--mysql", "Server=127.0.0.1;Port=3306;Database=ranking;User ID=bench;Password=bench;Maximum Pool Size=64"));
    }
}

readonly record struct Stats(double Avg, double P50, double P95, double P99, double Max)
{
    public static Stats From(double[] samples)
    {
        var s = (double[])samples.Clone();
        Array.Sort(s);
        double P(double q) => s[Math.Min(s.Length - 1, (int)Math.Ceiling(q * s.Length) - 1)];
        return new Stats(s.Average(), P(0.50), P(0.95), P(0.99), s[^1]);
    }
}
