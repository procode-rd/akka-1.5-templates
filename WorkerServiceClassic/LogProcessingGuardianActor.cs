using Akka.Actor;
using Akka.Event;
using Akka.Pattern;

namespace WorkerServiceClassic;

public sealed class LogProcessingGuardianActor : ReceiveActor
{
    public record WatchFolderMessage(string Path);
        
    private IActorRef _databaseWriterActorRef;
    private IActorContext _selfActorContext;

    public LogProcessingGuardianActor()
    {
        _databaseWriterActorRef = Context.System.DeadLetters;
        _selfActorContext = Context;
        
        Receive<WatchFolderMessage>(StartWatchFolderMessageHandler);
    }

    private void StartWatchFolderMessageHandler(WatchFolderMessage msg)
    {
        // e.g. conn string taken from config or so....
        string connectionString = "this is dummy as \"DatabaseWriter\" will emulate real database here... with some random exceptions...";

        if (_databaseWriterActorRef.Equals(Context.System.DeadLetters))
        {
            _databaseWriterActorRef = Context.ActorOf(
                props: BackoffSupervisor.PropsWithSupervisorStrategy(
                    childProps: DatabaseWriterActor.ByProps(connectionString),
                    childName: "DatabaseWriter",
                    minBackoff: TimeSpan.Zero,
                    maxBackoff: TimeSpan.FromSeconds(10),
                    randomFactor: 0,
                    strategy:new OneForOneStrategy(DatabaseWriterSupervisorDecider, loggingEnabled: true)),
                name: "DatabaseWriterSupervisor");
        }

        IActorRef fileWatcherActorRef = Context.ActorOf(
            props: BackoffSupervisor.PropsWithSupervisorStrategy(
                childProps: FileWatcherActor.ByProps(msg.Path, LogProcessorActorProvider),
                childName: BuildActorName("FileWatcher_", msg.Path),
                minBackoff: TimeSpan.Zero, 
                maxBackoff: TimeSpan.Zero, 
                randomFactor: 0,
                strategy: new OneForOneStrategy(FileWatcherSupervisionDecider, loggingEnabled: true)),
            name: "FileWatcherSupervisor");
        
        Context.GetLogger().Info("New actor watching file path: {0} of name `{1}` has been started", msg.Path, fileWatcherActorRef.Path);
    }
    
    private static string BuildActorName(string prefix, string fileSystemPath)
        => string.Concat(prefix, new string(fileSystemPath.Select(x => char.IsLetterOrDigit(x) || ActorPath.ValidSymbols.Contains(x) ? x : '$').ToArray()));
    
    private IActorRef LogProcessorActorProvider(string path)
    {
        string actorName = BuildActorName("LogProcessor_", Path.GetFileName(path));
        
        IActorRef child = _selfActorContext.Child(actorName);

        if (!child.Equals(Nobody.Instance))
        {
            return child;
        }
        
        return _selfActorContext.ActorOf(
            BackoffSupervisor.PropsWithSupervisorStrategy(
                childProps: LogProcessorActor.ByProps(_databaseWriterActorRef),
                childName: actorName,
                minBackoff: TimeSpan.Zero, 
                maxBackoff: TimeSpan.Zero,
                randomFactor: 0,
                strategy: new OneForOneStrategy(LogProcessorSupervisorDecider, loggingEnabled: true)),
            name: actorName + "_Supervisor");
    }
    
    private Directive FileWatcherSupervisionDecider(Exception ex)
    {
        switch (ex)
        {
            case InternalBufferOverflowException:
                // too much changes recorded, some will be discarded
                return Directive.Resume;
            
            case IOException:
                // handle to recorded file is lost
                return Directive.Resume;
            
            case OutOfMemoryException:
                // some serious shait
                return Directive.Escalate;
            
            default:
                // any other
                return Directive.Restart;
        }
    }
    
    private Directive LogProcessorSupervisorDecider(Exception ex)
    {
        switch (ex)
        {
            case FormatException:
                // line cannot be processed; does not parse; etc.
                return Directive.Resume;
            
            default:
                // other error, e.g. IOException of any other - rest of file cannot be processed - will be skipped
                return Directive.Restart;
        }
    }
    
    private Directive DatabaseWriterSupervisorDecider(Exception ex)
    {
        switch (ex)
        {
            case DatabaseException dbe:
                return dbe.Type switch
                {
                    DatabaseExceptionType.Transient => Directive.Resume,
                    DatabaseExceptionType.ErrorInQuery => Directive.Resume,
                    _ => Directive.Restart,
                };
                
            default:
                return Directive.Escalate;
        }
    }

}