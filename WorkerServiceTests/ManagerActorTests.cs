using System.Text.RegularExpressions;
using Akka.Actor;
using Akka.Event;
using Akka.TestKit;
using Akka.TestKit.Xunit2;
using Moq;
using WorkerServiceClassic;
using Xunit.Abstractions;

namespace AkkaWorkerTests;

public class ManagerActorTests : TestKit
{
    public ManagerActorTests(ITestOutputHelper output) : base(output : output)
    {
    }
    
    [Fact]
    public void When_StartMessage_IsReceived_Then_N_WorkerActorsChildren_AreSpawned()
    {
        // arrange
        var startMessage = new StartMessage(["a", "b", "c"]);

        var workersCreated = 0;
        
        var actorFactoryMock = new Mock<IActorRefFactory>();
        actorFactoryMock
            .Setup(x => x.ActorOf(It.Is<Props>(t => t.Type == typeof(WorkerActor)), It.IsAny<string>()))
            .Returns((Props p, string name) =>
            {
                Interlocked.Increment(ref workersCreated);
                return ActorOf(p, name);
            });
        
        var props = Props.Create(() => new ManagerActor(actorFactoryMock.Object));
        var tested = ActorOf(props, "tested");

        // act
        tested.Tell(startMessage, TestActor);
        
        // assert
        AwaitCondition(() => workersCreated == startMessage.Texts.Length, TimeSpan.FromSeconds(3));
    }

    [Fact]
    public void When_Worker_RespondsTo_ParseMessage_Within_ThreeSeconds_Then_SuccessMessage_IsLogged()
    {
        // arrange
        var startMessage = new StartMessage(["a", "b", "c"]);

        var probe = CreateTestProbe("worker");
        probe.SetAutoPilot(new DelegateAutoPilot(
            (sender, message) =>
            {
                Output.WriteLine($"Probe seen message: `{message.GetType().FullName}` from `{sender.Path}`");
                switch (message)
                {
                    case ParseMessage parseMessage:
                        sender.Tell(new DoneMessage(parseMessage.Text, parseMessage.Text), probe.Ref);
                        Output.WriteLine("Sent back DoneMessage");
                        break;
                }

                return AutoPilot.KeepRunning;
            }));

        var successMessageEventFilter = EventFilter.Custom(x => x is Info { Message: LogMessage { Format: "text {0} has been parsed by: {1}" } });

        var actorFactoryMock = new Mock<IActorRefFactory>();
        actorFactoryMock
            .Setup(x => x.ActorOf(It.Is<Props>(t => t.Type == typeof(WorkerActor)), It.IsAny<string>()))
            .Returns((Props _, string _) => probe.Ref);

        var props = Props.Create(() => new ManagerActor(actorFactoryMock.Object));
        var tested = ActorOf(props, "tested");

        // act, assert
        successMessageEventFilter.Expect(expectedCount: startMessage.Texts.Length, timeout: TimeSpan.FromSeconds(3), () => tested.Tell(startMessage, TestActor));
    }
    
    [Fact]
    public void When_Worker_RespondsTo_ParseMessage_After_ThreeSeconds_Then_FailedMessage_IsLogged()
    {
        // arrange
        var startMessage = new StartMessage(["a", "b", "c"]);

        var probe = CreateTestProbe("worker");
        probe.SetAutoPilot(new DelegateAutoPilot(
            (sender, message) =>
            {
                Output.WriteLine($"Probe seen message: `{message.GetType().FullName}` from `{sender.Path}`");
                switch (message)
                {
                    case ParseMessage parseMessage:
                        Thread.Sleep(TimeSpan.FromSeconds(3.1));
                        sender.Tell(new DoneMessage(parseMessage.Text, parseMessage.Text), probe.Ref);
                        Output.WriteLine("Sent back DoneMessage");
                        break;
                }

                return AutoPilot.KeepRunning;
            }));

        var successMessageEventFilter = EventFilter.Custom(x => x is Info { Message: LogMessage { Format: "parsing of text {0} has failed being processed by {1}" } });

        var actorFactoryMock = new Mock<IActorRefFactory>();
        actorFactoryMock
            .Setup(x => x.ActorOf(It.Is<Props>(t => t.Type == typeof(WorkerActor)), It.IsAny<string>()))
            .Returns((Props _, string _) => probe.Ref);

        var props = Props.Create(() => new ManagerActor(actorFactoryMock.Object));
        var tested = ActorOf(props, "tested");

        // act, assert
        successMessageEventFilter.Expect(expectedCount: startMessage.Texts.Length, timeout: TimeSpan.FromSeconds(3 * startMessage.Texts.Length + 1), () => tested.Tell(startMessage, TestActor));
    }    
}