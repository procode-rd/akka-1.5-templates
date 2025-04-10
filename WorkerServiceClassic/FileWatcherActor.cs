using System.Runtime.CompilerServices;
using Akka.Actor;
using Akka.Event;

namespace WorkerServiceClassic;

public sealed class FileWatcherActor : ReceiveActor
{
    private readonly string _path;
    private readonly Func<string, IActorRef> _logProcessorActorProvider;
    private readonly ILoggingAdapter _logger;
    private readonly FileSystemWatcher _fileSystemWatcher;

    public static Props ByProps(string path, Func<string, IActorRef> logProcessorActorFactory) 
        => Props.Create(() => new FileWatcherActor(path, logProcessorActorFactory));

    public FileWatcherActor(string path, Func<string, IActorRef> logProcessorActorProvider)
    {
        _path = path;
        _logProcessorActorProvider = logProcessorActorProvider;
        _logger = Context.GetLogger();

        _fileSystemWatcher = new FileSystemWatcher(path);
        _fileSystemWatcher.NotifyFilter = NotifyFilters.LastWrite;
        _fileSystemWatcher.Filter = "*.log";
        _fileSystemWatcher.IncludeSubdirectories = true;
        _fileSystemWatcher.Changed += FileChangedEventHandler;
        _fileSystemWatcher.Error += ErrorEventHandler;
        _fileSystemWatcher.EnableRaisingEvents = true;
    }
    
    private void FileChangedEventHandler(object sender, FileSystemEventArgs e)
        => StartProcessingFile(e.FullPath);
    
    private void StartProcessingFile(string path, [CallerMemberName] string? caller = null)
    {
        _logger.Info("Detected change through {0} in: {1}", caller, path);
        
        _logProcessorActorProvider(path).Tell(new LogProcessorActor.ProcessFileMessage(path));
    }
    
    private void ErrorEventHandler(object sender, ErrorEventArgs e)
    {
        Exception error = e.GetException();
        
        _logger.Error(error, "Error processing watcher on path: {0}", _path);
        throw error;
    }
}