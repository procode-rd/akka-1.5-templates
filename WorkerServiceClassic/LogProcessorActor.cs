using Akka.Actor;
using Akka.Event;

namespace WorkerServiceClassic;

public sealed class LogProcessorActor : ReceiveActor
{
    public record ProcessFileMessage(string Path);
        
    public static Props ByProps(IActorRef databaseWriterActorRef)
        => Props.Create(() => new LogProcessorActor(databaseWriterActorRef));

    private readonly IActorRef _databaseWriterActorRef;
    
    public LogProcessorActor(IActorRef databaseWriterActorRef)
    {
        _databaseWriterActorRef = databaseWriterActorRef;

        Receive<ProcessFileMessage>(ProcessFileMessageHandler);
    }

    private void ProcessFileMessageHandler(ProcessFileMessage msg)
    {
        int linesProcessed = 0;
        
        foreach (string line in File.ReadAllLines(msg.Path))
        {
            _databaseWriterActorRef.Tell(new DatabaseWriterActor.WriteDatabaseLogLine(line));
            linesProcessed++;
        }
        
        Context.GetLogger().Info("Processed {0} lines from {1}", linesProcessed, msg.Path);
    }
}