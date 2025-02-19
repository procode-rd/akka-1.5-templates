using Akka.Actor;
using Akka.Hosting;

namespace WorkerServiceDI;

public class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;
    private readonly IRequiredActor<ClockActor> _clockActor;
    private readonly ActorSystem _actorSystem;

    public Worker(ILogger<Worker> logger, ActorSystem actorSystem, IRequiredActor<ClockActor> clockActor)
    {
        _logger = logger;
        _actorSystem = actorSystem;
        _clockActor = clockActor;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Starting worker");

        // some action to be done 
        IActorRef clockActorRef = await _clockActor.GetAsync(stoppingToken);

        _actorSystem.Scheduler.ScheduleTellRepeatedly(
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
}