using Akka.Actor;
using Akka.Event;

namespace WorkerServiceClassic;

public sealed class DatabaseWriterActor : ReceiveActor, IWithUnboundedStash
{
    public sealed record WriteDatabaseLogLine(string logLine); 
    
    public static Props ByProps(string connectionString)
        => Props.Create(() => new DatabaseWriterActor(connectionString));

    public IStash Stash { get; set; }
    
    public DatabaseWriterActor(string connectionString)
    {
        // _connection = new SqlConnection(_connectionString); <- for real connection to use or alike
        
        Receive<WriteDatabaseLogLine>(WriteDatabaseLogLineHandler);
    }

    private void WriteDatabaseLogLineHandler(WriteDatabaseLogLine msg)
    {
        // dummy exception generation :D

        string line = msg.logLine.ToLowerInvariant();
        
        if (line.Contains("server_crash"))
            throw new DatabaseException(DatabaseExceptionType.Fatal, "Database server crashed");
        if (line.Contains("invalid_query"))
            throw new DatabaseException(DatabaseExceptionType.ErrorInQuery, "Invalid query");
        if (line.Contains("conn_lost"))
            throw new DatabaseException(DatabaseExceptionType.Transient, "Connection lost");
        
        // no exception, we are lucky :D
        Context.GetLogger().Debug("Line written to database: {0}", msg.logLine);
    }

    protected override void PreRestart(Exception reason, object message)
    {
        if (reason is DatabaseException dbEx && dbEx.Type == DatabaseExceptionType.Transient && message is WriteDatabaseLogLine oldMessage)
        {
            WriteDatabaseLogLine newMessage = RecoveryFromPoisonousMessage(oldMessage);
            Stash.Prepend([new Envelope(newMessage, Self)]);
            Stash.Unstash();
        }

        base.PreRestart(reason, message);
    }

    private WriteDatabaseLogLine RecoveryFromPoisonousMessage(WriteDatabaseLogLine oldMessage)
    {
        return new WriteDatabaseLogLine(oldMessage.logLine.Replace("conn_lost", "<recovered_from_c_lost>"));
    }
    
    public override void AroundPostRestart(Exception cause, object message)
    {
        base.AroundPostRestart(cause, message);
        
        // recovery after restart when transient connection error occured with concrete message type
        switch (cause)
        {
            case DatabaseException dbEx when dbEx.Type == DatabaseExceptionType.Transient:
                Context.GetLogger().Debug("Resuming from transient error with reprocessing");
                break;
            case DatabaseException dbEx when dbEx.Type == DatabaseExceptionType.ErrorInQuery:
                Context.GetLogger().Debug("Resuming from query error ignoring last message");
                break;
            default:
                Context.GetLogger().Debug("Resuming from other cause: {0}", cause.GetType().FullName);
                break;
        }
    }

    protected override void PostStop()
    {
        Context.GetLogger().Debug("PostStop - I'm stopped");
        base.PostStop();
    }
}