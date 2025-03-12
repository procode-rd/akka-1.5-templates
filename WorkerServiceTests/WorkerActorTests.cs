using System.Diagnostics;
using Akka.TestKit.Xunit2;
using WorkerServiceClassic;
using Xunit.Abstractions;

namespace AkkaWorkerTests;

public class WorkerActorTests : TestKit
{
    public WorkerActorTests(ITestOutputHelper output) : base(output: output)
    {
    }

    [Fact]
    public void When_ParseMessage_IsReceived_Then_DoneMessageIsSendToTheSender_Between_TwoAndFourSeconds()
    {
        // arrange
        var data = new ParseMessage("abc");

        var tested = ActorOf<WorkerActor>("tested");
        
        // act
        var sw = Stopwatch.StartNew();
        tested.Tell(data, sender: TestActor);
        
        // assert
        ExpectMsg<DoneMessage>(x => sw.Elapsed >= TimeSpan.FromSeconds(2),TimeSpan.FromSeconds(4.1));
    }
    
    [Theory,
        InlineData("abc", "abc"),
        InlineData("--xyz--", "xyz"),
        InlineData("this too-", "this too"),
    ]
    public void When_ParseMessage_IsReceived_Then_DoneMessage_ContainsInputTextWithoutHyphens(string input, string expected)
    {
        // arrange
        var data = new ParseMessage(input);

        var tested = ActorOf<WorkerActor>("tested");
        
        // act
        var sw = Stopwatch.StartNew();
        tested.Tell(data, sender: TestActor);
        
        // assert
        ExpectMsg<DoneMessage>(x => x.Parsed == expected,TimeSpan.FromSeconds(4.1));
    }
    
}