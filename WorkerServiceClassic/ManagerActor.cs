using Akka.Actor;
using Akka.Event;

namespace WorkerServiceClassic;

public class ManagerActor : ReceiveActor
{
    public ManagerActor()
    {
        Receive<StartMessage>(StartMessageHandler);
        Receive<DoneMessage>(DoneMessageHandler);
    }

    private void StartMessageHandler(StartMessage data)
    {
        var logger = Context.GetLogger();
        
        for (int i = 0; i < data.Texts.Length; i++)
        {
            string workerName = $"worker-{i}";
            IActorRef worker = Context.ActorOf<WorkerActor>(workerName);
            
            logger.Info("sending text {0} to worker {1}", data.Texts[i], worker);
            
            worker.Tell(new ParseMessage(data.Texts[i]));
        }
    }

    private void DoneMessageHandler(DoneMessage data)
    {
        var logger = Context.GetLogger();
        
        logger.Info("text {0} has been finished: {1}", data.Input, data.Parsed);
    }
}