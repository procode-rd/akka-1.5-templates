using System.Data.Common;

namespace WorkerServiceClassic;

public enum DatabaseExceptionType
{
    Transient = 0,
    ErrorInQuery,
    Fatal,
}

public sealed class DatabaseException : DbException
{
    public DatabaseException(DatabaseExceptionType type, string message) : base(message)
    {
        Type = type;
    }
    
    public DatabaseExceptionType Type { get; } 
}