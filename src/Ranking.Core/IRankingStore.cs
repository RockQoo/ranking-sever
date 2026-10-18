namespace Ranking.Core;

/// <summary>순위 한 줄. Rank는 1부터 시작한다.</summary>
public readonly record struct RankEntry(int UserId, long Score, long Rank);

/// <summary>
/// 랭킹 저장소. 정렬 기준은 두 구현 모두 같다:
/// 점수 내림차순, 점수가 같으면 user_id 내림차순.
/// (Redis ZSET은 동점이면 member 사전순이라 member를 고정 폭 문자열로 저장해 맞춘다)
/// </summary>
public interface IRankingStore : IAsyncDisposable
{
    string Name { get; }

    /// <summary>데이터를 모두 지우고 빈 랭킹으로 만든다.</summary>
    Task ResetAsync();

    /// <summary>초기 데이터를 한꺼번에 넣는다.</summary>
    Task BulkLoadAsync(IReadOnlyList<(int UserId, long Score)> rows);

    /// <summary>점수를 갱신한다. 없으면 추가한다.</summary>
    Task SetScoreAsync(int userId, long score);

    /// <summary>내 순위. 랭킹에 없으면 null.</summary>
    Task<long?> GetRankAsync(int userId);

    /// <summary>상위 n명.</summary>
    Task<IReadOnlyList<RankEntry>> GetTopAsync(int n);

    /// <summary>offset번째(0부터)부터 count명. "더 보기"·깊은 페이지 조회.</summary>
    Task<IReadOnlyList<RankEntry>> GetPageAsync(long offset, int count);

    /// <summary>내 위아래 range명씩 (나 포함).</summary>
    Task<IReadOnlyList<RankEntry>> GetAroundAsync(int userId, int range);

    /// <summary>전체 인원.</summary>
    Task<long> CountAsync();
}
