using Akka.Actor;

namespace WorkerServiceClassic;

public class Guardian : BackgroundService
{
    private readonly ILogger<Guardian> _logger;

    public Guardian(ILogger<Guardian> logger)
    {
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Starting guardian");
        
        ActorSystem guardian = ActorSystem.Create("Guardian", "akka.loglevel=DEBUG");
        try
        {
            _logger.LogInformation("setting up. Creating manager");
            
            IActorRef managerRef = guardian.ActorOf(Props.Create<ManagerActor>(), "Manager");

            managerRef.Tell(new StartMessage(["--one--", "--two--"]));
            
            while (!stoppingToken.IsCancellationRequested)
            {
                // keep it running as long as wanted
                await Task.Delay(1000, stoppingToken);
            }
        }
        finally
        {
            await guardian.Terminate();
        }
    }
}