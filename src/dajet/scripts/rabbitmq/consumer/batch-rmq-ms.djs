
PRIVATE @buffer  array
PRIVATE @headers object

SET @headers.version = '1.0'

CONSUME 'amqp://guest:guest@localhost:5672'
   WITH QueueName = 'test-queue'
      , Heartbeat = 10, PrefetchCount = 100
   INTO @buffer

USE TRANSACTION 'MS_TEST'

  INSERT РегистрСведений.ВходящаяОчередь FROM @buffer
  SELECT Отправитель    = @buffer.AppId
       , ДатаВремя      = NOW()
       , НомерСообщения = VECTOR('so_import')
       , ТипСообщения   = @buffer.Type
       , ТелоСообщения  = @buffer.Body
       , ТекстОшибки    = JSON(@buffer.Headers)

  --THROW '[ERROR][TEST] Broken transaction'
END

--THROW '[ERROR][TEST] Losing messages from the queue'