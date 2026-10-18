using MySqlConnector;

namespace Ranking.Core;

/// <summary>
/// 저장 프로시저 호출 도우미. SP 본문은 sql/procedures.sql에 있다.
/// call은 "CALL sp_xxx(@a, @b)" 형태로 쓰고, 인자는 (이름, 값) 쌍으로 넘긴다.
/// </summary>
internal static class MySqlProc
{
    public static async Task ExecuteAsync(MySqlDataSource dataSource, string call, params (string Name, object Value)[] args)
    {
        await using var conn = await dataSource.OpenConnectionAsync();
        await using var cmd = Command(conn, call, args);
        await cmd.ExecuteNonQueryAsync();
    }

    public static async Task<object?> ScalarAsync(MySqlDataSource dataSource, string call, params (string Name, object Value)[] args)
    {
        await using var conn = await dataSource.OpenConnectionAsync();
        await using var cmd = Command(conn, call, args);
        var result = await cmd.ExecuteScalarAsync();
        return result is DBNull ? null : result;
    }

    public static async Task<List<T>> ReadAsync<T>(MySqlDataSource dataSource, string call, Func<MySqlDataReader, T> map, params (string Name, object Value)[] args)
    {
        await using var conn = await dataSource.OpenConnectionAsync();
        await using var cmd = Command(conn, call, args);
        var result = new List<T>();
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
            result.Add(map(reader));
        return result;
    }

    public static MySqlCommand Command(MySqlConnection conn, string call, params (string Name, object Value)[] args)
    {
        var cmd = new MySqlCommand(call, conn) { CommandTimeout = 0 };
        foreach (var (name, value) in args)
            cmd.Parameters.AddWithValue(name, value);
        return cmd;
    }
}
