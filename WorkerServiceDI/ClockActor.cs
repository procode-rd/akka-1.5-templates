using Akka.Actor;
using Akka.Event;

namespace WorkerServiceDI;

public class ClockActor : ReceiveActor
{
    public ClockActor()
    {
        Receive<string>(TimeQuestionHandler, msg => msg == "What time is it?");
    }

    private void TimeQuestionHandler(string message)
    {
        Context.GetLogger().Debug("The time is now {0}", DateTime.Now);
    }
}