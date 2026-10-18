using DaJet.Scripting;
using DaJet.Scripting.Model;

namespace DaJet.Kafka
{
    public sealed class Consumer : ProcessorBase
    {
        private readonly ScriptContext _context;
        private readonly ConsumeStatement _statement;
        public Consumer(in ScriptContext context, in ConsumeStatement statement)
        {
            ArgumentNullException.ThrowIfNull(context, nameof(context));
            ArgumentNullException.ThrowIfNull(statement, nameof(statement));

            _context = context;
            _statement = statement;
        }
        public override ExitCode Process()
        {
            throw new NotImplementedException();
        }
        public override void Dispose()
        {
            throw new NotImplementedException();
        }
    }
}