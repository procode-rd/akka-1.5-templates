using Akka.Actor;

namespace WorkerServiceClassic;

public class GuardianBackgroundService : BackgroundService
{
    private readonly ILogger<GuardianBackgroundService> _logger;

    public GuardianBackgroundService(ILogger<GuardianBackgroundService> logger)
    {
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Starting guardian");

        string hocon = await File.ReadAllTextAsync("app.conf", stoppingToken);

        ActorSystem guardian = ActorSystem.Create("Guardian", hocon);
        try
        {
            _logger.LogInformation("setting up. Creating manager");
            
            IActorRef logProcessingGuardianRef = guardian.ActorOf(Props.Create<LogProcessingGuardianActor>(), "LogProcessingGuardian");

            logProcessingGuardianRef.Tell(new LogProcessingGuardianActor.WatchFolderMessage(Environment.CurrentDirectory));
            
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