using System.Diagnostics;
using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using StackExchange.Redis;

// Kafka → Redis 랭킹 반영 데모.
//   게임 서버(프로듀서)가 "점수 획득" 이벤트를 user_id 키로 발행하고,
//   랭킹 갱신기(컨슈머 그룹)가 받아서 Redis ZSET에 ZINCRBY로 더한다.
// 확인하는 것:
//   1) 같은 키(user_id)는 같은 파티션 → 유저별 순서 보장
//   2) at-least-once: 반영 후 커밋 전에 죽으면 재시작 때 같은 이벤트를 다시 받는다
//   3) 그래서 반영은 멱등이어야 한다 — 유저별 seq를 Lua로 확인하고 더한다. 안 하면 점수가 중복으로 더해진다
//
// 사용법: dotnet run -c Release --project src/Ranking.Kafka -- [--users 10000] [--events 200000]

var options = DemoOptions.Parse(args);
var report = new StringBuilder();
void Log(string line = "")
{
    Console.WriteLine(line);
    report.AppendLine(line);
}

const string Topic = "score-events";
const string Group = "ranking-updater";
const int Partitions = 4;
const int CommittedBeforeCrash = 20_000;  // 1단계 컨슈머가 커밋까지 끝낸 이벤트 수
const int AppliedBeforeCrash = 30_000;    // 1단계 컨슈머가 Redis에 반영한 이벤트 수 (차이만큼 커밋 안 하고 죽는다)

Log("# Kafka → Redis 랭킹 반영 데모");
Log();
Log($"- 일시: {DateTime.Now:yyyy-MM-dd HH:mm}");
Log($"- 토픽 `{Topic}` 파티션 {Partitions}개, 컨슈머 그룹 `{Group}`");
Log($"- 이벤트 {options.Events:N0}건 (유저 {options.Users:N0}명, 점수 획득량 1~100), 키 = user_id");
Log($"- 서버: WSL2 Ubuntu의 Kafka 4.3.1 (KRaft 단일 노드), Redis 8.0.5. 클라이언트: Windows .NET {Environment.Version}, Confluent.Kafka");
Log();

// ---- 준비: 토픽과 Redis 키 초기화
using (var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = options.Kafka }).Build())
{
    var existing = admin.GetMetadata(TimeSpan.FromSeconds(10)).Topics.Any(t => t.Topic == Topic);
    if (existing)
    {
        await admin.DeleteTopicsAsync([Topic]);
        // 삭제는 비동기라 사라질 때까지 기다린다
        while (admin.GetMetadata(TimeSpan.FromSeconds(10)).Topics.Any(t => t.Topic == Topic && t.Error.Code == ErrorCode.NoError))
            await Task.Delay(200);
    }
    for (var attempt = 0; ; ++attempt)
    {
        try
        {
            await admin.CreateTopicsAsync([new TopicSpecification { Name = Topic, NumPartitions = Partitions, ReplicationFactor = 1 }]);
            break;
        }
        catch (CreateTopicsException) when (attempt < 50)
        {
            await Task.Delay(200);  // 삭제가 아직 진행 중
        }
    }
}

await using var mux = await ConnectionMultiplexer.ConnectAsync(options.Redis);
var redis = mux.GetDatabase();
RedisKey safeKey = "ranking:kafka:safe";      // seq 확인 후 반영 (멱등)
RedisKey seqKey = "ranking:kafka:safe:seq";   // 유저별 마지막으로 반영한 seq
RedisKey naiveKey = "ranking:kafka:naive";    // 받은 대로 바로 ZINCRBY (멱등 아님)
await redis.KeyDeleteAsync([safeKey, seqKey, naiveKey]);

// ---- 이벤트 생성. seq는 유저별로 0부터 1씩 증가 (유저를 소유한 게임 서버가 매긴다고 가정)
var rng = new Random(42);
var events = new ScoreEvent[options.Events];
var nextSeq = new long[options.Users + 1];
var expected = new long[options.Users + 1];
for (var i = 0; i < events.Length; ++i)
{
    var userId = rng.Next(1, options.Users + 1);
    var delta = rng.Next(1, 101);
    events[i] = new ScoreEvent(userId, delta, nextSeq[userId]++);
    expected[userId] += delta;
}

// ---- 1. 발행
Log("## 1. 발행 (acks=all, 프로듀서 멱등성 켬)");
Log();
var partitionCounts = new long[Partitions];
var deliveryErrors = 0;
var sw = Stopwatch.StartNew();
using (var producer = new ProducerBuilder<int, string>(new ProducerConfig
{
    BootstrapServers = options.Kafka,
    Acks = Acks.All,
    EnableIdempotence = true,   // 프로듀서 재전송으로 브로커에 중복이 쌓이는 것을 막는다 (컨슈머 쪽 중복과는 별개)
    LingerMs = 5,
}).Build())
{
    void OnDelivered(DeliveryReport<int, string> dr)
    {
        if (dr.Error.IsError)
            Interlocked.Increment(ref deliveryErrors);
        else
            Interlocked.Increment(ref partitionCounts[dr.Partition.Value]);
    }

    foreach (var e in events)
    {
        var message = new Message<int, string> { Key = e.UserId, Value = JsonSerializer.Serialize(e) };
        while (true)
        {
            try
            {
                producer.Produce(Topic, message, OnDelivered);
                break;
            }
            catch (ProduceException<int, string> ex) when (ex.Error.Code == ErrorCode.Local_QueueFull)
            {
                producer.Poll(TimeSpan.FromMilliseconds(50));   // 로컬 송신 큐가 찼다. 전송이 진행되길 기다렸다가 다시 넣는다
            }
        }
    }
    producer.Flush(TimeSpan.FromSeconds(60));
}
sw.Stop();
Log($"- {events.Length:N0}건 / {sw.Elapsed.TotalSeconds:F2}s = **초당 {events.Length / sw.Elapsed.TotalSeconds:N0}건**, 전송 실패 {deliveryErrors}건");
Log($"- 파티션별 건수: {string.Join(", ", partitionCounts.Select((c, p) => $"P{p}={c:N0}"))} (키 해시로 고르게 나뉜다)");
Log();

// ---- 2. 컨슈머 A: 반영하다가 커밋 전에 죽는다
Log($"## 2. 컨슈머 A — {AppliedBeforeCrash:N0}건 반영, {CommittedBeforeCrash:N0}건까지만 커밋하고 종료 (장애 재현)");
Log();
var a = await RunConsumerAsync("A", stopAfterApplied: AppliedBeforeCrash, commitLimit: CommittedBeforeCrash);
Log($"- A: 받은 이벤트 {a.Received:N0}, 반영 {a.Applied:N0}, 커밋 {a.Committed:N0}, 받은 파티션 {a.Partitions}");
Log($"- 커밋 안 된 {a.Received - a.Committed:N0}건은 **이미 Redis에 반영됐지만** 브로커는 처리 안 된 것으로 안다");
Log();

// ---- 3. 컨슈머 B1, B2: 같은 그룹으로 재시작. 커밋된 위치부터 다시 받는다
Log("## 3. 컨슈머 B1·B2 — 같은 그룹으로 동시에 시작해 남은 이벤트 처리");
Log();
sw.Restart();
// Consume()가 블로킹이라 각자 스레드에서 돌린다
var bTasks = new[] { Task.Run(() => RunConsumerAsync("B1")), Task.Run(() => RunConsumerAsync("B2")) };
var bs = await Task.WhenAll(bTasks);
sw.Stop();
var bReceived = bs.Sum(b => b.Received);
foreach (var b in bs)
    Log($"- {b.Name}: 받은 이벤트 {b.Received:N0}, 반영 {b.Applied:N0}, 중복이라 건너뜀 {b.Skipped:N0}, 받은 파티션 {b.Partitions}, 순서 위반 {b.OrderViolations}");
Log($"- 합계 {bReceived:N0}건 / {(sw.Elapsed - options.IdleTimeout).TotalSeconds:F2}s (대기 시간 제외) ≈ 초당 {bReceived / Math.Max(0.001, (sw.Elapsed - options.IdleTimeout).TotalSeconds):N0}건 (Redis 반영·커밋 포함)");
Log($"- 다시 받은 이벤트 = B가 받은 {bReceived:N0} − A가 아직 안 받은 {events.Length - a.Received:N0} = **{bReceived - (events.Length - a.Received):N0}건** (A가 반영만 하고 커밋 못 한 만큼)");
Log();

// ---- 4. 결과 검증
Log("## 4. 최종 점수 검증 (기대값 = 유저별 점수 획득량 합)");
Log();
var safeMismatch = 0;
var naiveMismatch = 0;
long naiveOver = 0;
var batch = redis.CreateBatch();
var safeTasks = new Task<double?>[options.Users + 1];
var naiveTasks = new Task<double?>[options.Users + 1];
for (var u = 1; u <= options.Users; ++u)
{
    safeTasks[u] = batch.SortedSetScoreAsync(safeKey, Member(u));
    naiveTasks[u] = batch.SortedSetScoreAsync(naiveKey, Member(u));
}
batch.Execute();
for (var u = 1; u <= options.Users; ++u)
{
    var safe = (long)(await safeTasks[u] ?? 0);
    var naive = (long)(await naiveTasks[u] ?? 0);
    if (safe != expected[u])
        ++safeMismatch;
    if (naive != expected[u])
    {
        ++naiveMismatch;
        naiveOver += naive - expected[u];
    }
}
Log("| 반영 방식 | 점수가 틀린 유저 | 초과 지급된 점수 합 |");
Log("|---|---:|---:|");
Log($"| seq 확인 후 반영 (Lua, 멱등) | {safeMismatch:N0} | 0 |");
Log($"| 받은 대로 ZINCRBY | {naiveMismatch:N0} | {naiveOver:N0} |");
Log();
Log("> 다시 받은 이벤트는 멱등 처리 덕분에 seq 확인에서 걸러지고, 멱등 처리가 없으면 그만큼 점수가 두 번 더해진다.");

var dir = Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "..", "..", "results");
Directory.CreateDirectory(dir);
var file = Path.GetFullPath(Path.Combine(dir, $"{DateTime.Now:yyyyMMdd-HHmm}-kafka.md"));
await File.WriteAllTextAsync(file, report.ToString());
Console.WriteLine($"결과 저장: {file}");
return safeMismatch == 0 ? 0 : 1;

// ----------------------------------------------------------------------------

static RedisValue Member(int userId) => userId.ToString("D10");

// 컨슈머 하나를 돌린다. 배치로 받아 Redis에 파이프라인으로 반영하고, 반영이 끝난 뒤 커밋한다(at-least-once).
//   stopAfterApplied: 이만큼 반영하면 커밋 없이 종료 (장애 재현용)
//   commitLimit: 반영 누적이 이 값을 넘는 배치부터는 커밋하지 않는다
async Task<ConsumerStats> RunConsumerAsync(string name, int stopAfterApplied = int.MaxValue, int commitLimit = int.MaxValue)
{
    // 유저별 seq를 확인하고, 처음 보는 seq일 때만 더한다. Redis는 스크립트를 원자적으로 실행한다
    const string applyScript = """
        local last = tonumber(redis.call('HGET', KEYS[2], ARGV[1]) or '-1')
        if tonumber(ARGV[3]) <= last then return 0 end
        redis.call('ZINCRBY', KEYS[1], ARGV[2], ARGV[1])
        redis.call('HSET', KEYS[2], ARGV[1], ARGV[3])
        return 1
        """;

    var st = new ConsumerStats(name);
    var assigned = new SortedSet<int>();
    var consumedFrom = new SortedSet<int>();
    var lastSeqSeen = new Dictionary<int, long>();

    using var consumer = new ConsumerBuilder<int, string>(new ConsumerConfig
    {
        BootstrapServers = options.Kafka,
        GroupId = Group,
        AutoOffsetReset = AutoOffsetReset.Earliest,
        EnableAutoCommit = false,           // 반영이 끝난 뒤 직접 커밋한다
        PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky,
    })
    .SetPartitionsAssignedHandler((_, parts) => { foreach (var p in parts) assigned.Add(p.Partition.Value); })
    .SetPartitionsRevokedHandler((_, parts) => { foreach (var p in parts) assigned.Remove(p.Partition.Value); })
    .Build();

    consumer.Subscribe(Topic);
    var idleSince = Stopwatch.StartNew();
    var buffer = new List<ConsumeResult<int, string>>(options.BatchSize);

    while (st.Applied < stopAfterApplied)
    {
        buffer.Clear();
        while (buffer.Count < options.BatchSize && st.Applied + buffer.Count < stopAfterApplied)
        {
            var cr = consumer.Consume(TimeSpan.FromMilliseconds(100));
            if (cr is null)
                break;
            buffer.Add(cr);
        }

        if (buffer.Count == 0)
        {
            // 그룹 합류(리밸런스)에 몇 초 걸리므로, 파티션을 받기 전에는 더 오래 기다린다
            var timeout = assigned.Count > 0 ? options.IdleTimeout : TimeSpan.FromSeconds(30);
            if (idleSince.Elapsed > timeout)
                break;   // 더 받을 게 없다
            continue;
        }
        idleSince.Restart();

        // 같은 연결로 순서대로 보내므로 한 유저의 이벤트는 받은 순서대로 반영된다
        var applyTasks = new List<Task<RedisResult>>(buffer.Count);
        var naiveTasks = new List<Task<double>>(buffer.Count);
        foreach (var cr in buffer)
        {
            consumedFrom.Add(cr.Partition.Value);
            var e = JsonSerializer.Deserialize<ScoreEvent>(cr.Message.Value)!;
            if (lastSeqSeen.TryGetValue(e.UserId, out var last) && e.Seq <= last)
                ++st.OrderViolations;   // 한 컨슈머 안에서는 일어나면 안 된다 (파티션 내 순서 보장)
            lastSeqSeen[e.UserId] = e.Seq;

            applyTasks.Add(redis.ScriptEvaluateAsync(applyScript, [safeKey, seqKey], [Member(e.UserId), e.Delta, e.Seq]));
            naiveTasks.Add(redis.SortedSetIncrementAsync(naiveKey, Member(e.UserId), e.Delta));
        }
        await Task.WhenAll(naiveTasks);
        foreach (var r in await Task.WhenAll(applyTasks))
        {
            if ((int)r == 1)
                ++st.Applied;
            else
                ++st.Skipped;
        }
        st.Received += buffer.Count;

        if (st.Received <= commitLimit)
        {
            // 파티션마다 마지막으로 처리한 오프셋 + 1을 커밋
            var offsets = buffer
                .GroupBy(cr => cr.TopicPartition)
                .Select(g => new TopicPartitionOffset(g.Key, g.Max(cr => cr.Offset.Value) + 1))
                .ToList();
            consumer.Commit(offsets);
            st.Committed = st.Received;
        }
    }

    st.Partitions = consumedFrom.Count == 0 ? "-" : string.Join(",", consumedFrom.Select(p => $"P{p}"));
    consumer.Close();   // 자동 커밋이 꺼져 있어 커밋하지 않은 오프셋은 그대로 남는다
    return st;
}

record ScoreEvent(int UserId, int Delta, long Seq);

sealed class ConsumerStats(string name)
{
    public string Name { get; } = name;
    public long Received;
    public long Applied;
    public long Skipped;
    public long Committed;
    public long OrderViolations;
    public string Partitions = "-";
}

sealed record DemoOptions(int Users, int Events, int BatchSize, TimeSpan IdleTimeout, string Kafka, string Redis)
{
    public static DemoOptions Parse(string[] args)
    {
        string Get(string name, string fallback)
        {
            var i = Array.IndexOf(args, name);
            return i >= 0 && i + 1 < args.Length ? args[i + 1] : fallback;
        }

        return new DemoOptions(
            Users: int.Parse(Get("--users", "10000")),
            Events: int.Parse(Get("--events", "200000")),
            BatchSize: int.Parse(Get("--batch", "500")),
            IdleTimeout: TimeSpan.FromSeconds(5),
            Kafka: Get("--kafka", "127.0.0.1:9092"),
            Redis: Get("--redis", "127.0.0.1:6379"));
    }
}
