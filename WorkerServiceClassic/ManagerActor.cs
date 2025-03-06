using Akka.Actor;
using Akka.Event;

namespace WorkerServiceClassic;

public class ManagerActor : ReceiveActor
{
    private record SuccessMessage(DoneMessage Message, IActorRef WorkerRef);

    private record FailedMessage(string Input, IActorRef WorkerRef);
    
    public ManagerActor()
    {
        Receive<StartMessage>(StartMessageHandler);
        Receive<SuccessMessage>(SuccessHandler);
        Receive<FailedMessage>(FailedHandler);
    }

    private void StartMessageHandler(StartMessage data)
    {
        var logger = Context.GetLogger();

        var self = Context.Self;
        
        for (int i = 0; i < data.Texts.Length; i++)
        {
            string workerName = $"worker-{i}";
            IActorRef workerRef = Context.ActorOf<WorkerActor>(workerName);
            
            string input = data.Texts[i];
            logger.Info("sending text {0} to worker {1}", input, workerRef);

            _ = workerRef
                .Ask<DoneMessage>(new ParseMessage(input), TimeSpan.FromSeconds(3))
                .ContinueWith(x =>
                {
                    if (x.IsCompletedSuccessfully)
                    {
                        self.Tell(new SuccessMessage(x.Result, workerRef));
                        return;
                    }
                    
                    self.Tell(new FailedMessage(input, workerRef));
                });
        }
    }

    private void SuccessHandler(SuccessMessage data)
    {
        var logger = Context.GetLogger();
        
        logger.Info("text {0} has been parsed by: {1}", data.Message.Parsed, data.WorkerRef);
    }
    
    private void FailedHandler(FailedMessage data)
    {
        var logger = Context.GetLogger();
        
        logger.Info("parsing of text {0} has failed being processed by {1}", data.Input, data.WorkerRef);
    }
    
}