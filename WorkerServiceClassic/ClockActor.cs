using Akka.Actor;
using Akka.Event;

namespace WorkerServiceClassic;

public class ClockActor : ReceiveActor
{
    public static Props Props() => Akka.Actor.Props.Create(() => new ClockActor());
    
    public ClockActor()
    {
        Receive<string>(TimeQuestionHandler, msg => msg == "What time is it?");
    }

    private void TimeQuestionHandler(string message)
    {
        Context.GetLogger().Debug("The time is now {0}", DateTime.Now);
    }
}