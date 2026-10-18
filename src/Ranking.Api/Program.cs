using Ranking.Core;

// 저장소는 설정 Ranking:Backend 로 고른다 (redis | mysql).
// 예: dotnet run --project src/Ranking.Api -- --Ranking:Backend=mysql
var builder = WebApplication.CreateBuilder(args);
var config = builder.Configuration.GetSection("Ranking");
var backend = config["Backend"] ?? "redis";

IRankingStore store = backend switch
{
    "redis" => await RedisRankingStore.ConnectAsync(config["Redis"] ?? "127.0.0.1:6379"),
    "mysql" => new MySqlRankingStore(config["MySql"] ?? throw new InvalidOperationException("Ranking:MySql 설정이 없다")),
    _ => throw new InvalidOperationException($"알 수 없는 Backend: {backend}"),
};
builder.Services.AddSingleton(store);

var app = builder.Build();

app.MapGet("/", () => $"ranking-server ({store.Name})");

app.MapPut("/scores/{userId:int}", async (int userId, ScoreRequest req, IRankingStore s) =>
{
    if (req.Score < 0)
        return Results.BadRequest("score는 0 이상");
    await s.SetScoreAsync(userId, req.Score);
    return Results.NoContent();
});

app.MapGet("/ranks/{userId:int}", async (int userId, IRankingStore s) =>
    await s.GetRankAsync(userId) is { } rank ? Results.Ok(new { userId, rank }) : Results.NotFound());

app.MapGet("/top", async (int? n, IRankingStore s) =>
    Results.Ok(await s.GetTopAsync(Math.Clamp(n ?? 100, 1, 1000))));

app.MapGet("/around/{userId:int}", async (int userId, int? range, IRankingStore s) =>
{
    var entries = await s.GetAroundAsync(userId, Math.Clamp(range ?? 5, 1, 50));
    return entries.Count > 0 ? Results.Ok(entries) : Results.NotFound();
});

app.Lifetime.ApplicationStopping.Register(() => store.DisposeAsync().AsTask().Wait());
app.Run();

record ScoreRequest(long Score);
