using Akka.Actor;
using Akka.Event;
using Akka.Routing;

namespace WorkerServiceClassic;

public class ManagerActor : ReceiveActor
{
    private record ResponseMessage;
    
    private record SuccessMessage(DoneMessage Message, IActorRef WorkerRef) : ResponseMessage;

    private record FailedMessage(string Input, IActorRef WorkerRef) : ResponseMessage;
    
    public ManagerActor()
    {
        // pick one to test
        //Receive<StartMessage>(StartMessageHandlerAsInBook);
        //ReceiveAsync<StartMessage>(StartMessageHandlerSimpleAsync);
        //ReceiveAsync<StartMessage>(StartMessageHandlerRobustAsync);
        Receive<StartMessage>(StartMessageHandlerAkkaWay);
        
        Receive<SuccessMessage>(SuccessHandler);
        Receive<FailedMessage>(FailedHandler);
    }
    
    /// <summary>
    /// Handler whose implementation is as close as in the book  
    /// </summary>
    private void StartMessageHandlerAsInBook(StartMessage data)
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

    /// <summary>
    /// Simple (readable) async implementation 
    /// </summary>
    private async Task StartMessageHandlerSimpleAsync(StartMessage data)
    {
        var logger = Context.GetLogger();

        for (int i = 0; i < data.Texts.Length; i++)
        {
            string workerName = $"worker-{i}";
            IActorRef workerRef = Context.ActorOf<WorkerActor>(workerName);

            string input = data.Texts[i];
            logger.Info("sending text {0} to worker {1}", input, workerRef);

            try
            {
                var result = await workerRef.Ask<DoneMessage>(new ParseMessage(input), TimeSpan.FromSeconds(3));

                Self.Tell(new SuccessMessage(result, workerRef));
            }
            catch (AskTimeoutException)
            {
                Self.Tell(new FailedMessage(input, workerRef));
            }
        }
    }

    /// <summary>
    /// More performant async implementation 
    /// </summary>
    private async Task StartMessageHandlerRobustAsync(StartMessage data)
    {
        int i = 0;

        Task<ResponseMessage>[] tasks = data
            .Texts
            .Select<string, Task<ResponseMessage>>(async input =>
            {
                string workerName = $"worker-{i++}";
                IActorRef workerRef = Context.ActorOf<WorkerActor>(workerName);

                try
                {
                    var result = await workerRef.Ask<DoneMessage>(new ParseMessage(input), TimeSpan.FromSeconds(3));
                    
                    return new SuccessMessage(result, workerRef); 
                }
                catch (AskTimeoutException)
                {
                    return new FailedMessage(input, workerRef);
                }
            })
            .ToArray();

        await Task.WhenAll(tasks);

        foreach (var task in tasks)
        {
            Self.Tell(task.Result);
        }
    }
    
    /// <summary>
    /// More natural way of processing in parallel by Akka. BUT we will see more in next chapters.
    /// Here I've added this only to provide some comparisons in how implementation could/would look. 
    /// </summary>
    private void StartMessageHandlerAkkaWay(StartMessage data)
    {
        Props workerActorProps = Props
            .Create<WorkerActor>()
            .WithRouter(new RoundRobinPool(nrOfInstances: 10));
            
        IActorRef workerActorPoolRef = Context.ActorOf(workerActorProps, "worker");
        IActorRef self = Context.Self;

        foreach (string input in data.Texts)
        {
            workerActorPoolRef
                .Ask<DoneMessage>(new ParseMessage(input), TimeSpan.FromSeconds(3))
                .ContinueWith(task =>
                    self.Tell(task.IsCompletedSuccessfully
                        ? new SuccessMessage(task.Result, workerActorPoolRef)
                        : new FailedMessage(input, workerActorPoolRef)));
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