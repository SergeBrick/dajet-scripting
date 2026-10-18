using Confluent.Kafka;
using DaJet.Data;
using DaJet.Scripting;
using DaJet.Scripting.Model;
using DaJet.Utilities;
using System.Text;
using static System.Runtime.InteropServices.JavaScript.JSType;
using Error = Confluent.Kafka.Error;

namespace DaJet.Kafka
{
    public sealed class Producer : ProcessorBase
    {
        private readonly ScriptContext _context;
        private readonly ProduceStatement _statement;
        private readonly Dictionary<string, SyntaxNode> _select = new();
        private readonly Dictionary<string, SyntaxNode> _options = new();
        private int _produced;
        private int _persisted;
        private string _error;
        private string _topic;
        private readonly ProducerConfig _config;
        private IProducer<byte[], byte[]> _producer;
        private readonly Message<byte[], byte[]> _message = new();
        private readonly Action<IProducer<byte[], byte[]>, Error> _errorHandler;
        private readonly Action<IProducer<byte[], byte[]>, LogMessage> _logHandler;
        private readonly Action<DeliveryReport<byte[], byte[]>> _deliveryHandler;
        public Producer(in ScriptContext context, in ProduceStatement statement)
        {
            ArgumentNullException.ThrowIfNull(context, nameof(context));
            ArgumentNullException.ThrowIfNull(statement, nameof(statement));

            _context = context;
            _statement = statement;

            foreach (ColumnExpression column in _statement.Options)
            {
                _options.Add(column.Alias, column.Expression);
            }

            foreach (ColumnExpression column in _statement.Columns)
            {
                _select.Add(column.Alias, column.Expression);
            }

            _logHandler = LogHandler;
            _errorHandler = ErrorHandler;
            _deliveryHandler = DeliveryHandler;

            _config = CreateProducerConfig();
        }
        private ProducerConfig CreateProducerConfig()
        {
            Dictionary<string, string> config = new();

            foreach (KeyValuePair<string, SyntaxNode> option in _options)
            {
                SyntaxNode expression = option.Value;

                object value = _context.Evaluate(in expression);

                config.Add(option.Key, value.ToString());
            }

            return new ProducerConfig(config);
        }
        private byte[] GetKey()
        {
            if (_select.TryGetValue("Key", out SyntaxNode expression))
            {
                object value = _context.Evaluate(in expression);

                if (value is byte[] binary)
                {
                    return binary;
                }
                else if (value is string text && !string.IsNullOrEmpty(text))
                {
                    return Encoding.UTF8.GetBytes(text);
                }
            }
            
            return null;
        }
        private byte[] GetValue()
        {
            if (_select.TryGetValue("Value", out SyntaxNode expression))
            {
                object value = _context.Evaluate(in expression);

                if (value is byte[] binary)
                {
                    return binary;
                }
                else if (value is string text && !string.IsNullOrEmpty(text))
                {
                    return Encoding.UTF8.GetBytes(text);
                }
            }

            return Array.Empty<byte>();
        }
        private string GetTopic()
        {
            if (_select.TryGetValue("Topic", out SyntaxNode expression))
            {
                object value = _context.Evaluate(in expression);

                return value.ToString();
            }

            return string.Empty;
        }
        public override ExitCode Process()
        {
            if (_context.IsCancellationRequested)
            {
                return ExitCode.Cancel;
            }

            if (_producer is null) // initial state of processor
            {
                if (_context.GetDataSource() is DataSourceScope scope)
                {
                    scope.OnCommit += SynchronizeCommit;
                    scope.OnCancel += SynchronizeCancel;
                }

                _producer = new ProducerBuilder<byte[], byte[]>(_config)
                    .SetLogHandler(_logHandler)
                    .SetErrorHandler(_errorHandler)
                    .Build();
            }
            
            ExitCode code = ExitCode.Success;

            try
            {
                ThrowIfStateIsBroken();

                _topic = GetTopic();
                _message.Key = GetKey();
                _message.Value = GetValue();

                _producer.Produce(_topic, _message, _deliveryHandler); // async inside - returns immediately

                _ = Interlocked.Increment(ref _produced);
            }
            catch (Exception error)
            {
                string message = ExceptionHelper.GetErrorMessage(error);

                FileLogger.Default.Write($"[ERROR][DaJet.Kafka.Producer][{_config.ClientId}][{_topic}] {message}");

                ResetState(); throw;
            }

            return code;
        }
        private void LogHandler(IProducer<byte[], byte[]> _, LogMessage message)
        {
            FileLogger.Default.Write($"[INFO][DaJet.Kafka.Producer][{_config.ClientId}][{_topic}] {message.Name}: {message.Message}");
        }
        private void ErrorHandler(IProducer<byte[], byte[]> _, Error error)
        {
            _error = error.Reason;

            FileLogger.Default.Write($"[ERROR][DaJet.Kafka.Producer][{_config.ClientId}][{_topic}] {_error}");
        }
        private void DeliveryHandler(DeliveryReport<byte[], byte[]> report)
        {
            if (report.Status == PersistenceStatus.Persisted)
            {
                _ = Interlocked.Increment(ref _persisted); return; // successful delivery
            }

            if (report.Error is not null && report.Error.Code != ErrorCode.NoError)
            {
                _error = report.Error.Reason;

                FileLogger.Default.Write($"[ERROR][DaJet.Kafka.Producer][{_config.ClientId}][{report.Topic}] {_error}");
            }
        }
        private void ThrowIfStateIsBroken()
        {
            string error = _error;

            if (error is not null)
            {
                string message = $"[BROKEN][DaJet.Kafka.Producer][{_config.ClientId}][{_topic}] Persisted {_persisted} of {_produced} messages.";

                FileLogger.Default.Write(message);

                throw new InvalidOperationException(message);
            }
        }
        public void SynchronizeCommit(object sender, EventArgs args)
        {
            if (_context.IsCancellationRequested)
            {
                return;
            }

            bool success = true;
            
            try
            {
                ThrowIfStateIsBroken();

                // Wait until all outstanding produce requests and delivery report callbacks are completed.
                _producer.Flush(_context.Cancellation);

                if (_produced != _persisted) // Something went wrong: throw an exception to trigger a database transaction rollback.
                {
                    throw new InvalidOperationException($"[ERROR][DaJet.Kafka.Producer][{_config.ClientId}][{_topic}] Failed to confirm {_produced - _persisted} of {_produced} messages.");
                }
            }
            catch
            {
                success = false; throw; // Failed commit: Cancel will be called
            }
            finally
            {
                if (success) // Successful commit: Cancel won't be called
                {
                    SynchronizeCancel(sender, args);
                }
            }
        }
        private void SynchronizeCancel(object sender, EventArgs args)
        {
            if (sender is DataSourceScope scope)
            {
                scope.OnCommit -= SynchronizeCommit;
                scope.OnCancel -= SynchronizeCancel;
            }

            if (_produced == _persisted)
            {
                FileLogger.Default.Write($"[SUCCESS][DaJet.Kafka.Producer][{_config.ClientId}][{_topic}] Produced {_persisted} messages.");
            }
            else
            {
                FileLogger.Default.Write($"[WARNING][DaJet.Kafka.Producer][{_config.ClientId}][{_topic}] Persisted {_persisted} of {_produced} messages.");
            }

            ResetState();
        }
        private void ResetState()
        {
            _error = null;
            _produced = 0;
            _persisted = 0;
            _message.Key = null;
            _message.Value = null;

            try { _producer?.Dispose(); }
            catch { /* ignore errors */ }
            finally { _producer = null; }
        }
        public override void Dispose()
        {
            //TODO: think about processor's lifecycle

            // Commit and Cancel methods will never be called
            // if processor is not bound to a data source (scoped)
        }
    }
}