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
)
from profiler import profile
from loguru import logger
import asyncio


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
                change_dict = self._change_to_dict(change)

                message = aio_pika.Message(
                    body=json.dumps(change_dict).encode(),
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,  # Для сохранения сообщения при перезагрузке
                )

                await self.channel.default_exchange.publish(
                    message, routing_key=self.queue_name
                )

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
                        data = json.loads(body)
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
    async def get_message(self) -> Optional[TimetableChangeData]:
        try:
            await self.initialize()

            message = await self.queue.get(no_ack=False)
            if message:
                try:
                    body = message.body.decode()
                    data = json.loads(body)
                    await message.ack()

                    return self.dict_to_change(data)
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

    @profile(func_name="broker_change_to_dict")
    def _change_to_dict(self, change: TimetableChangeData) -> Dict[str, Any]:
        entity_dict = {
            "type": change.entity.type.value,
            "id": change.entity.id,
            "name": change.entity.name,
        }

        metadata_changes = []
        if change.metadata_changes:
            for field_change in change.metadata_changes:
                metadata_changes.append(
                    {
                        "field_name": field_change.field_name,
                        "old_value": str(field_change.old_value),
                        "new_value": str(field_change.new_value),
                    }
                )

        lesson_changes = []
        if change.lesson_changes:
            for lesson_change in change.lesson_changes:
                field_changes = []
                for field_change in lesson_change.field_changes:
                    field_changes.append(
                        {
                            "field_name": field_change.field_name,
                            "old_value": str(field_change.old_value),
                            "new_value": str(field_change.new_value),
                        }
                    )

                lesson_changes.append(
                    {
                        "change_type": lesson_change.change_type.value,
                        "field_changes": field_changes,
                    }
                )

        return {
            "entity": entity_dict,
            "metadata_changes": metadata_changes,
            "lesson_changes": lesson_changes,
            "timestamp": str(asyncio.get_event_loop().time()),
        }

    @staticmethod
    @profile(func_name="broker_dict_to_change")
    def dict_to_change(data: Dict[str, Any]) -> TimetableChangeData:
        entity_data = data.get("entity", {})
        entity = Entity(
            type=EntityType(entity_data.get("type")),
            id=entity_data.get("id"),
            name=entity_data.get("name"),
        )

        metadata_changes = []
        for change_data in data.get("metadata_changes", []) or []:
            field_change = FieldChange(
                field_name=change_data.get("field_name"),
                old_value=change_data.get("old_value"),
                new_value=change_data.get("new_value"),
            )
            metadata_changes.append(field_change)

        lesson_changes = []
        for lesson_change_data in data.get("lesson_changes", []) or []:
            field_changes = []
            for field_change_data in lesson_change_data.get("field_changes", []) or []:
                field_change = FieldChange(
                    field_name=field_change_data.get("field_name"),
                    old_value=field_change_data.get("old_value"),
                    new_value=field_change_data.get("new_value"),
                )
                field_changes.append(field_change)

            lesson_change = LessonChange(
                change_type=ChangeType(lesson_change_data.get("change_type")),
                field_changes=field_changes,
            )
            lesson_changes.append(lesson_change)

        return TimetableChangeData(
            entity=entity,
            metadata_changes=metadata_changes if metadata_changes else None,
            lesson_changes=lesson_changes if lesson_changes else None,
        )
