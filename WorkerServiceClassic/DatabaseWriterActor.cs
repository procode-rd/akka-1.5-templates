using Akka.Actor;
using Akka.Event;

namespace WorkerServiceClassic;

public sealed class DatabaseWriterActor : ReceiveActor
{
    public sealed record WriteDatabaseLogLine(string logLine); 
    
    private readonly string _connectionString;

    public static Props ByProps(string connectionString)
        => Props.Create(() => new DatabaseWriterActor(connectionString));

    public DatabaseWriterActor(string connectionString)
    {
        _connectionString = connectionString;
        
        // _connection = new SqlConnection(_connectionString); <- for real connection to use
        
        Receive<WriteDatabaseLogLine>(WriteDatabaseLogLineHandler);
    }

    private void WriteDatabaseLogLineHandler(WriteDatabaseLogLine msg)
    {
        // dummy exception generation :D

        switch (new Random().Next(20))
        {
            case <= 16:
                // no exception, we are lucky :D
                Context.GetLogger().Debug("Line written to database: {0}", msg.logLine);
                break;
            case 17:
                throw new DatabaseException(DatabaseExceptionType.Transient, "Connection lost");
            case 18:
                throw new DatabaseException(DatabaseExceptionType.ErrorInQuery, "Invalid query");
            default:
                throw new DatabaseException(DatabaseExceptionType.Fatal, "Database server crashed");
        }
    }
    
    public override void AroundPostRestart(Exception cause, object message)
    {
        base.AroundPostRestart(cause, message);

        // recovery after restart when transient connection error occured with concrete message type 
        if (cause is DatabaseException { Type: DatabaseExceptionType.Transient } && message is WriteDatabaseLogLine writeLineMessage)
        {
            Context.GetLogger().Debug("Resuming from transient error");
            
            WriteDatabaseLogLineHandler(writeLineMessage);
        }
    }
}