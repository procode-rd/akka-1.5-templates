using Akka.Actor;
using Akka.Configuration;
using Akka.Logger.Serilog;

namespace WorkerServiceClassic;

public class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;

    public Worker(ILogger<Worker> logger)
    {
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Starting worker");
        
        ActorSystem actorSystem = ActorSystem.Create("WorkerActorSystemClassic", "akka.loglevel=DEBUG");
        try
        {
            // some action to be done 
            IActorRef clockActorRef = actorSystem.ActorOf(ClockActor.Props(), "Clock");

            actorSystem.Scheduler.ScheduleTellRepeatedly(
                initialDelay: TimeSpan.FromSeconds(1),
                interval: TimeSpan.FromSeconds(1),
                receiver: clockActorRef,
                message: "What time is it?",
                sender: Nobody.Instance);
            // end of some action

            while (!stoppingToken.IsCancellationRequested)
            {
                // keep it running as long as wanted
                await Task.Delay(1000, stoppingToken);
            }
        }
        finally
        {
            await actorSystem.Terminate();
        }
    }
}