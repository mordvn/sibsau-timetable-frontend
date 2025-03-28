import aio_pika
import json
from typing import List, Dict, Any, Callable, Optional, Union
from parser_types import (
    TimetableChangeData,
    Entity,
    EntityType,
    FieldChange,
    ChangeType,
    LessonChange,
    Lesson,
    ScheduleType,
    ScheduleForm,
    WeekNumber,
    DayName,
    LessonType,
    Subgroup,
)
from profiler import profile
from logger import trace
from loguru import logger
import asyncio
from datetime import date, time, datetime, timedelta
from enum import Enum


class DataEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Enum):
            return {"__enum__": obj.value}
        if isinstance(obj, (date, datetime)):
            return {"__datetime__": obj.isoformat()}
        if isinstance(obj, time):
            return {"__time__": obj.isoformat()}
        if isinstance(obj, timedelta):
            return {"__timedelta__": obj.total_seconds()}
        if isinstance(obj, Lesson):
            return {"__lesson__": obj.__dict__}
        if isinstance(obj, Entity):
            return {"__entity__": obj.__dict__}
        if isinstance(obj, FieldChange):
            return {"__fieldchange__": obj.__dict__}
        if isinstance(obj, LessonChange):
            return {"__lessonchange__": obj.__dict__}
        if isinstance(obj, TimetableChangeData):
            return {"__timetablechangedata__": obj.__dict__}
        return super().default(obj)


class Broker:
    def __init__(self, connection_string: str, queue_name: str = "timetable_changes"):
        self.connection_string = connection_string
        self.queue_name = queue_name
        self.connection = None
        self.channel = None
        self.initialized = False
        self.consumer_tag = None
        self.queue = None

    async def __aenter__(self):
        await self.initialize()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    @profile(func_name="broker_initialize")
    async def initialize(self):
        if not self.initialized:
            try:
                self.connection = await aio_pika.connect_robust(self.connection_string)
                self.channel = await self.connection.channel()

                # Создаем очередь с durable=True для сохранения сообщений при перезагрузке
                self.queue = await self.channel.declare_queue(
                    self.queue_name, durable=True
                )

                self.initialized = True
                logger.debug("Подключение к RabbitMQ инициализировано успешно")
            except Exception as e:
                logger.error(f"Ошибка инициализации подключения к RabbitMQ: {e}")
                if self.connection:
                    await self.connection.close()
                self.connection = None
                self.channel = None
                self.queue = None
                self.initialized = False
                raise e

    @profile(func_name="broker_send_changes")
    async def send_changes(self, changes: List[TimetableChangeData]) -> bool:
        if not changes:
            return True

        try:
            await self.initialize()

            for change in changes:
                try:
                    message = aio_pika.Message(
                        body=json.dumps(change, cls=DataEncoder).encode(),
                        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                    )

                    await self.channel.default_exchange.publish(
                        message, routing_key=self.queue_name
                    )
                except Exception as e:
                    logger.error(f"Ошибка при сериализации/отправке изменения: {e}")
                    import traceback
                    logger.error(traceback.format_exc())
                    raise

            return True
        except Exception as e:
            logger.error(f"Ошибка отправки изменений в RabbitMQ: {e}")
            return False

    @profile(func_name="broker_start_consuming")
    async def start_consuming(
        self, callback: Callable[[Dict[str, Any]], Union[bool, None]]
    ):
        try:
            await self.initialize()

            async def process_message(message: aio_pika.IncomingMessage):
                async with message.process():
                    try:
                        body = message.body.decode()
                        data = Broker.process_message(body)
                        logger.debug(f"Получено сообщение из RabbitMQ: {data}")

                        result = (
                            await callback(data)
                            if asyncio.iscoroutinefunction(callback)
                            else callback(data)
                        )

                        if result is False:
                            await message.reject(requeue=True)
                            logger.debug("Сообщение отклонено и возвращено в очередь")
                        else:
                            await message.ack()
                            logger.debug("Сообщение успешно обработано и подтверждено")
                    except Exception as e:
                        logger.error(f"Ошибка обработки сообщения: {e}")
                        await message.reject(requeue=True)
                        logger.debug(
                            "Сообщение отклонено из-за ошибки и возвращено в очередь"
                        )

            self.consumer_tag = await self.queue.consume(process_message)
            logger.info(f"Начато потребление сообщений из очереди {self.queue_name}")

        except Exception as e:
            logger.error(f"Ошибка при настройке потребления сообщений: {e}")
            raise

    @profile(func_name="broker_stop_consuming")
    async def stop_consuming(self):
        if self.channel and self.consumer_tag:
            await self.channel.cancel(self.consumer_tag)
            self.consumer_tag = None
            logger.info(
                f"Потребление сообщений из очереди {self.queue_name} остановлено"
            )

    @profile(func_name="broker_get_message")
    @trace
    async def get_message(self) -> Optional[TimetableChangeData]:
        try:
            await self.initialize()

            message = await self.queue.get(no_ack=False)
            if message:
                try:
                    body = message.body.decode()
                    data = Broker.process_message(body)
                    await message.ack()

                    return data
                except Exception as e:
                    logger.error(f"Ошибка при обработке полученного сообщения: {e}")
                    await message.reject(requeue=True)
            return None

        except aio_pika.exceptions.QueueEmpty:
            return None
        except Exception as e:
            logger.error(f"Ошибка при получении сообщения: {e}")
            return None

    @profile(func_name="broker_close")
    async def close(self):
        if self.consumer_tag:
            await self.stop_consuming()

        if self.connection:
            await self.connection.close()
            self.connection = None
            self.channel = None
            self.queue = None
            self.initialized = False

        logger.debug("RabbitMQ соединение закрыто")

    @staticmethod
    @profile(func_name="broker_process_message")
    def process_message(message_body):
        try:
            if isinstance(message_body, bytes):
                message_body = message_body.decode('utf-8')
                
            return json.loads(message_body, object_hook=Broker.object_hook)
        except Exception as e:
            logger.error(f"Ошибка обработки сообщения: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None

    @staticmethod
    def object_hook(obj):
        if "__enum__" in obj:
            for enum_type in [ScheduleType, ScheduleForm, WeekNumber, DayName, LessonType, Subgroup, EntityType, ChangeType]:
                try:
                    return enum_type(obj["__enum__"])
                except (ValueError, TypeError):
                    continue
            return obj["__enum__"]
        elif "__datetime__" in obj:
            return datetime.fromisoformat(obj["__datetime__"])
        elif "__time__" in obj:
            return time.fromisoformat(obj["__time__"])
        elif "__timedelta__" in obj:
            return timedelta(seconds=obj["__timedelta__"])
        elif "__lesson__" in obj:
            lesson_dict = obj["__lesson__"]
            # Обработка вложенных типов в Lesson
            for key, value in lesson_dict.items():
                if isinstance(value, dict):
                    lesson_dict[key] = Broker.object_hook(value)
                elif isinstance(value, list) and value and isinstance(value[0], dict):
                    lesson_dict[key] = [Broker.object_hook(item) for item in value]
            return Lesson(**lesson_dict)
        elif "__entity__" in obj:
            entity_dict = obj["__entity__"]
            for key, value in entity_dict.items():
                if isinstance(value, dict):
                    entity_dict[key] = Broker.object_hook(value)
            return Entity(**entity_dict)
        elif "__fieldchange__" in obj:
            fc_dict = obj["__fieldchange__"]
            for key, value in fc_dict.items():
                if isinstance(value, dict):
                    fc_dict[key] = Broker.object_hook(value)
            return FieldChange(**fc_dict)
        elif "__lessonchange__" in obj:
            lc_dict = obj["__lessonchange__"]
            for key, value in lc_dict.items():
                if isinstance(value, dict):
                    lc_dict[key] = Broker.object_hook(value)
                elif isinstance(value, list) and value and isinstance(value[0], dict):
                    lc_dict[key] = [Broker.object_hook(item) for item in value]
            return LessonChange(**lc_dict)
        elif "__timetablechangedata__" in obj:
            tcd_dict = obj["__timetablechangedata__"]
            for key, value in tcd_dict.items():
                if isinstance(value, dict):
                    tcd_dict[key] = Broker.object_hook(value)
                elif isinstance(value, list) and value and isinstance(value[0], dict):
                    tcd_dict[key] = [Broker.object_hook(item) for item in value]
            return TimetableChangeData(**tcd_dict)
        
        return obj
