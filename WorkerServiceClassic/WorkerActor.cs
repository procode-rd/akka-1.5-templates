using Akka.Actor;
using Akka.Event;

namespace WorkerServiceClassic;

public class WorkerActor : ReceiveActor
{
    public WorkerActor()
    {
        Receive<ParseMessage>(ParseMessageHandler);
    }

    private void ParseMessageHandler(ParseMessage message)
    {
        var logger = Context.GetLogger();

        var parsed = message.Text.Trim('-');
        logger.Info("Done! Parsed result: {0}", parsed);
        
        Context.Sender.Tell(new DoneMessage(message.Text, parsed));
    }
}