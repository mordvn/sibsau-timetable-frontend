from beanie import Document, init_beanie
from motor.motor_asyncio import AsyncIOMotorClient
from parser_types import (
    TimetableData,
    EntityType,
    Entity,
    Lesson,
    Metadata,
    WeekNumber,
    Semester,
    ScheduleType,
    ScheduleForm,
    DayName,
    LessonType,
    Subgroup,
)
from typing import List, Optional, Any
from pydantic import BaseModel
import pymongo
import pymongo.errors
import traceback
from loguru import logger
from datetime import time, timedelta, datetime
from profiler import profile
from cacher import Cacher
from config import settings

class LessonModel(BaseModel):
    schedule_type: str
    time_begin: str
    lesson_name: str
    schedule_form: Optional[str] = None
    week_number: Optional[str] = None
    day_name: Optional[str] = None
    day_date: Optional[Any] = None
    duration: Optional[int] = None
    lesson_type: Optional[str] = None
    groups: Optional[List[str]] = None
    professors: Optional[List[str]] = None
    auditorium: Optional[str] = None
    location: Optional[str] = None
    subgroups: str = ""


class MetadataModel(BaseModel):
    years: str
    date: Any
    week_number: str
    semester: Optional[str] = None


class EntityModel(BaseModel):
    type: str
    id: int
    name: Optional[str] = None


class TimetableModel(Document):
    entity: EntityModel
    metadata: MetadataModel
    lessons: List[LessonModel]

    class Settings:
        name = "timetables"
        use_revision = False
        indexes = [
            "entity.type",
            "entity.id",
            "entity.name",
            "metadata.week_number",
            "metadata.date",
            "metadata.semester",
        ]


class SubscriptionModel(Document):
    tg_id: int
    entity_name: str
    created_at: datetime = datetime.now()

    class Settings:
        name = "subscriptions"
        use_revision = False
        indexes = [
            "tg_id",
            "entity_name",
            [
                ("tg_id", pymongo.ASCENDING),
                ("entity_name", pymongo.ASCENDING),
            ],
        ]


class Database:
    def __init__(self, connection_string, db_name="sibsau-timetable"):
        self.connection_string = connection_string
        self.db_name = db_name
        self.client = None
        self.initialized = False

    async def __aenter__(self):
        await self.initialize()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    @profile(func_name="database_initialize")
    async def initialize(self):
        if not self.initialized:
            try:
                self.client = AsyncIOMotorClient(self.connection_string)

                db = self.client[self.db_name]

                await init_beanie(
                    database=db,
                    document_models=[TimetableModel, SubscriptionModel],
                    allow_index_dropping=True,
                )

                self.initialized = True
                logger.debug("MongoDB соединение инициализировано успешно")
            except Exception as e:
                logger.error(
                    f"Ошибка инициализации соединения MongoDB: {e}\n{traceback.format_exc()}"
                )
                if self.client:
                    self.client.close()
                self.client = None
                self.initialized = False
                raise e

    @profile(func_name="database_get_timetables")
    @Cacher.cache(expire=21600)
    async def get_timetables(self) -> List[TimetableData]:
        await self.initialize()
        models = await TimetableModel.find({}).to_list()
        return [self._from_model(model) for model in models]

    @profile(func_name="database_get_all_entities")
    @Cacher.cache(expire=21600)
    async def get_all_entities(self) -> List[Entity]:
        await self.initialize()

        collection = TimetableModel.get_motor_collection()
        entities_data = await collection.find({}, {"entity": 1, "_id": 0}).to_list(
            length=None
        )

        result = []
        for data in entities_data:
            entity_data = data.get("entity", {})
            result.append(
                Entity(
                    type=EntityType(entity_data.get("type")),
                    id=entity_data.get("id"),
                    name=entity_data.get("name"),
                )
            )
        return result

    @profile(func_name="database_get_timetable_by_query")
    @Cacher.cache(expire=21600)
    async def get_timetable_by_query(self, query: dict) -> Optional[TimetableData]:
        await self.initialize()
        model = await TimetableModel.find_one(query)
        if model:
            return self._from_model(model)
        return None

    @profile(func_name="database_delete_timetable")
    async def delete_timetable(self, entity_type: EntityType, entity_id: int) -> bool:
        await self.initialize()
        try:
            result = await TimetableModel.find_one(
                {"entity.type": entity_type.value, "entity.id": entity_id}
            )
            if result:
                await result.delete()
                logger.debug(f"Расписание удалено: {entity_type.value} {entity_id}")
                return True
            return False
        except Exception as e:
            logger.error(f"Ошибка удаления расписания: {e}")
            return False

    @profile(func_name="database_to_model")
    def _to_model(self, timetable: TimetableData) -> TimetableModel:
        if not timetable.entity:
            raise ValueError("Timetable Entity is None")
        if not timetable.entity.type:
            raise ValueError("Timetable Entity type is None")
        if not timetable.entity.id or timetable.entity.id <= 0:
            raise ValueError(f"Invalid Timetable Entity id: {timetable.entity.id}")

        entity_model = EntityModel(
            type=timetable.entity.type.value,
            id=timetable.entity.id,
            name=timetable.entity.name or "",
        )

        if entity_model.type is None or entity_model.id is None or entity_model.id <= 0:
            raise ValueError("Entity model validation error")

        if not timetable.metadata:
            raise ValueError("Timetable metadata is None")
        if not timetable.metadata.week_number:
            raise ValueError("Timetable metadata week_number is None")

        metadata_model = MetadataModel(
            years=timetable.metadata.years or "",
            date=timetable.metadata.date.date()
            if isinstance(timetable.metadata.date, datetime)
            else timetable.metadata.date,
            week_number=timetable.metadata.week_number.value,
            semester=timetable.metadata.semester.value
            if timetable.metadata.semester
            else None,
        )

        lesson_models = []
        for lesson in timetable.lessons:
            if not lesson.schedule_type:
                continue

            time_str = (
                lesson.time_begin.strftime("%H:%M") if lesson.time_begin else "00:00"
            )

            duration_seconds = (
                int(lesson.duration.total_seconds()) if lesson.duration else None
            )

            lesson_model = LessonModel(
                schedule_type=lesson.schedule_type.value,
                time_begin=time_str,
                lesson_name=lesson.lesson_name or "",
                schedule_form=lesson.schedule_form.value
                if lesson.schedule_form
                else None,
                week_number=lesson.week_number.value if lesson.week_number else None,
                day_name=lesson.day_name.value if lesson.day_name else None,
                day_date=lesson.day_date,
                duration=duration_seconds,
                lesson_type=lesson.lesson_type.value if lesson.lesson_type else None,
                groups=lesson.groups or [],
                professors=lesson.professors or [],
                auditorium=lesson.auditorium or "",
                location=lesson.location or "",
                subgroups=lesson.subgroups.value
                if lesson.subgroups
                else Subgroup.COMMON.value,
            )
            lesson_models.append(lesson_model)

        timetable_model = TimetableModel(
            entity=entity_model, metadata=metadata_model, lessons=lesson_models
        )

        if not hasattr(timetable_model, "entity") or timetable_model.entity is None:
            raise ValueError("Model validation error")

        return timetable_model

    @profile(func_name="database_from_model")
    def _from_model(self, model: TimetableModel) -> TimetableData:
        entity_obj = Entity(
            type=EntityType(model.entity.type),
            id=model.entity.id,
            name=model.entity.name,
        )

        metadata_obj = Metadata(
            years=model.metadata.years,
            date=model.metadata.date.date()
            if hasattr(model.metadata.date, "date")
            else model.metadata.date,
            week_number=WeekNumber(model.metadata.week_number),
            semester=Semester(model.metadata.semester)
            if model.metadata.semester
            else None,
        )

        def create_time_obj(time_str):
            try:
                return (
                    datetime.strptime(time_str, "%H:%M").time()
                    if time_str
                    else time(0, 0)
                )
            except ValueError:
                return time(0, 0)

        def create_duration_obj(duration):
            return timedelta(seconds=duration) if duration is not None else None

        lessons = []
        for lesson_model in model.lessons:
            time_obj = create_time_obj(lesson_model.time_begin)
            duration_obj = create_duration_obj(lesson_model.duration)

            lesson = Lesson(
                schedule_type=ScheduleType(lesson_model.schedule_type),
                time_begin=time_obj,
                lesson_name=lesson_model.lesson_name,
                schedule_form=ScheduleForm(lesson_model.schedule_form)
                if lesson_model.schedule_form
                else None,
                week_number=WeekNumber(lesson_model.week_number)
                if lesson_model.week_number
                else None,
                day_name=DayName(lesson_model.day_name)
                if lesson_model.day_name
                else None,
                day_date=lesson_model.day_date,
                duration=duration_obj,
                lesson_type=LessonType(lesson_model.lesson_type)
                if lesson_model.lesson_type
                else None,
                groups=lesson_model.groups or [],
                professors=lesson_model.professors or [],
                auditorium=lesson_model.auditorium or "",
                location=lesson_model.location or "",
                subgroups=Subgroup(lesson_model.subgroups)
                if lesson_model.subgroups
                else Subgroup.COMMON,
            )
            lessons.append(lesson)

        return TimetableData(entity=entity_obj, metadata=metadata_obj, lessons=lessons)

    @profile(func_name="database_close")
    async def close(self):
        if self.client and self.initialized:
            self.client.close()
            self.client = None
            self.initialized = False
            logger.debug("MongoDB соединение закрыто")

    @profile(func_name="database.user_subscribe")
    async def user_subscribe(self, tg_id: int, entity_name: str) -> bool:
        await self.initialize()

        try:
            existing = await SubscriptionModel.find_one(
                {"tg_id": tg_id, "entity_name": entity_name}
            )

            if existing:
                return True

            subscription = SubscriptionModel(
                tg_id=tg_id, entity_name=entity_name, created_at=datetime.now()
            )

            await subscription.insert()
            logger.debug(f"Пользователь {tg_id} подписался на {entity_name}")
            return True

        except Exception as e:
            logger.error(f"Ошибка при подписке пользователя: {e}")
            return False

    @profile(func_name="database.user_unsubscribe")
    async def user_unsubscribe(self, tg_id: int, entity_name: str) -> bool:
        await self.initialize()

        try:
            result = await SubscriptionModel.find_one(
                {"tg_id": tg_id, "entity_name": entity_name}
            )

            if result:
                await result.delete()
                logger.debug(f"Пользователь {tg_id} отписался от {entity_name}")
                return True

            return False

        except Exception as e:
            logger.error(f"Ошибка при отписке пользователя: {e}")
            return False

    @profile(func_name="database.user_is_subscribed")
    async def user_is_subscribed(self, tg_id: int, entity_name: str) -> bool:
        await self.initialize()

        try:
            count = await SubscriptionModel.find(
                {"tg_id": tg_id, "entity_name": entity_name}
            ).count()

            return count > 0

        except Exception as e:
            logger.error(f"Ошибка при проверке подписки пользователя: {e}")
            return False

    @profile(func_name="database.get_subscribed_users")
    async def get_subscribed_users(self, entity_name: str) -> List[int]:
        await self.initialize()

        try:
            subscriptions = await SubscriptionModel.find(
                {"entity_name": entity_name}
            ).to_list()

            return [subscription.tg_id for subscription in subscriptions]

        except Exception as e:
            logger.error(f"Ошибка при получении подписанных пользователей: {e}")
            return []


database = Database(settings.MONGODB_URI)
