using Akka.Actor;
using Akka.TestKit.Xunit2;
using Xunit.Abstractions;

namespace AkkaWorkerTests;

public class ClockActorTests : TestKit
{
    // for sake of having some test. normally, this would test real actors from referenced projects.
    public class LocalClockActor : ReceiveActor
    {
        public LocalClockActor()
        {
            Receive<string>(TimeQuestionHandler, msg => msg == "What time is it?");
        }

        private void TimeQuestionHandler(string message)
        {
            Context.Sender.Tell(DateTime.Now);
        }
    }    
    
    public ClockActorTests(ITestOutputHelper outputHelper) : base(output: outputHelper, config: "akka.loglevel = DEBUG")
    {
    } 
    
    [Fact]
    public async Task When_ClockActor_IsAskedForTime_Then_ItRespondsWithTime()
    {
        // arrange
        var tested = Sys.ActorOf(Props.Create<LocalClockActor>(), "clock-being-tested");

        // act
        tested.Tell("What time is it?");

        var result = await ExpectMsgAsync<DateTime>();
        
        // assert
        Assert.Equal(DateTime.Today, result.Date);
    }
}