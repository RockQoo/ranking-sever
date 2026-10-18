using StackExchange.Redis;

namespace Ranking.Core;

/// <summary>
/// Redis Sorted Set(ZSET) 랭킹. 내부는 skiplist + hash라
/// 점수 갱신(ZADD)과 순위 조회(ZREVRANK)가 모두 O(log N)이다.
/// </summary>
public sealed class RedisRankingStore : IRankingStore
{
    private readonly ConnectionMultiplexer _mux;
    private readonly IDatabase _db;
    private readonly RedisKey _key;

    public string Name => "redis";

    private RedisRankingStore(ConnectionMultiplexer mux, string key)
    {
        _mux = mux;
        _db = mux.GetDatabase();
        _key = key;
    }

    public static async Task<RedisRankingStore> ConnectAsync(string configuration, string key = "ranking:season")
    {
        var mux = await ConnectionMultiplexer.ConnectAsync(configuration);
        return new RedisRankingStore(mux, key);
    }

    // 동점일 때 ZSET은 member 사전순으로 정렬한다.
    // 고정 폭 숫자 문자열이면 사전순 == 숫자순이라 MySQL의 user_id 정렬과 같아진다.
    private static RedisValue Member(int userId) => userId.ToString("D10");
    private static int UserId(RedisValue member) => int.Parse(member.ToString());

    public Task ResetAsync() => _db.KeyDeleteAsync(_key);

    public async Task BulkLoadAsync(IReadOnlyList<(int UserId, long Score)> rows)
    {
        const int batchSize = 10_000;
        for (var i = 0; i < rows.Count; i += batchSize)
        {
            var count = Math.Min(batchSize, rows.Count - i);
            var entries = new SortedSetEntry[count];
            for (var j = 0; j < count; ++j)
            {
                var (userId, score) = rows[i + j];
                entries[j] = new SortedSetEntry(Member(userId), score);
            }
            await _db.SortedSetAddAsync(_key, entries);
        }
    }

    public Task SetScoreAsync(int userId, long score) =>
        _db.SortedSetAddAsync(_key, Member(userId), score);

    public async Task<long?> GetRankAsync(int userId)
    {
        var rank = await _db.SortedSetRankAsync(_key, Member(userId), Order.Descending);
        return rank + 1;
    }

    public async Task<IReadOnlyList<RankEntry>> GetTopAsync(int n)
    {
        var entries = await _db.SortedSetRangeByRankWithScoresAsync(_key, 0, n - 1, Order.Descending);
        return ToRankEntries(entries, 1);
    }

    // ZREVRANGE offset offset+count-1. skiplist의 span으로 offset 위치까지 O(log N)에 내려간다.
    public async Task<IReadOnlyList<RankEntry>> GetPageAsync(long offset, int count)
    {
        var entries = await _db.SortedSetRangeByRankWithScoresAsync(_key, offset, offset + count - 1, Order.Descending);
        return ToRankEntries(entries, offset + 1);
    }

    public async Task<IReadOnlyList<RankEntry>> GetAroundAsync(int userId, int range)
    {
        var rank = await _db.SortedSetRankAsync(_key, Member(userId), Order.Descending);
        if (rank is null)
            return [];

        var start = Math.Max(0, rank.Value - range);
        var stop = rank.Value + range;
        var entries = await _db.SortedSetRangeByRankWithScoresAsync(_key, start, stop, Order.Descending);
        return ToRankEntries(entries, start + 1);
    }

    public Task<long> CountAsync() => _db.SortedSetLengthAsync(_key);

    private static RankEntry[] ToRankEntries(SortedSetEntry[] entries, long firstRank)
    {
        var result = new RankEntry[entries.Length];
        for (var i = 0; i < entries.Length; ++i)
            result[i] = new RankEntry(UserId(entries[i].Element), (long)entries[i].Score, firstRank + i);
        return result;
    }

    public async ValueTask DisposeAsync() => await _mux.DisposeAsync();
}
