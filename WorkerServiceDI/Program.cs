using System.Diagnostics.CodeAnalysis;
using Akka.Actor;
using Akka.Hosting;
using Akka.Logger.Serilog;

namespace WorkerServiceDI;

public class Program
{
    public static void Main(string[] args)
    {
        var builder = Host.CreateApplicationBuilder(args);
        builder.Services.AddHostedService<Worker>();
        builder.Services.AddAkka("WorkerActorSystemDI", akkaBuilder =>
        {
            akkaBuilder
                .ConfigureLoggers(setup =>
                {
                    setup.LogLevel = Akka.Event.LogLevel.DebugLevel;
            
                    setup.ClearLoggers();
            
                    setup.AddDefaultLogger();
                    setup.AddLoggerFactory();
            
                    setup.AddLogger<SerilogLogger>();                
                })
                .WithActors((system, registry) =>
                {
                    Verify(registry.TryRegister<ClockActor>(system.ActorOf<ClockActor>("Clock")));
                });
        });

        var host = builder.Build();
        host.Run();
    }

    private static void Verify([DoesNotReturnIf(false)] bool mustSucceed)
    {
        if (!mustSucceed)
        {
            throw new InvalidOperationException("Could not register actor");
        }
    }
}