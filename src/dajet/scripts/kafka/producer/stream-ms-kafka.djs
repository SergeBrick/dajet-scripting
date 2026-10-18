
DECLARE @Отправитель string = 'MS_TEST'

PRIVATE @message object
PRIVATE @headers object
PRIVATE @Счётчик integer

SET @headers.version = '1.0'

USE 'MS_TEST'

   STREAM TOP 10 НомерСообщения
        , Отправитель, Получатель
        , ТипСообщения, ТелоСообщения
     INTO @message
     FROM РегистрСведений.ИсходящаяОчередь
    ORDER BY НомерСообщения ASC

   PRODUCE 'kafka'
      WITH acks               = 'all'
         , client.id          = 'dajet'
         , bootstrap.servers  = '192.168.61.221:9092' --  (csv)
         , max.in.flight      = 1
         , message.timeout.ms = 30000
         , enable.idempotence = false
    SELECT Topic = 'test-topic'
         , Value = JSON(@message)

   SET @Счётчик = @Счётчик + 1

   --IF @Счётчик = 5 THEN SLEEP 5 THROW 'TEST ERROR' END
END

PRINT '[STREAM] MS_TEST > KAFKA = ' + @Счётчик

RETURN '[STREAM] SUCCESS'