from typing import List, Optional, Dict, Any, Union, Callable
from parser_types import (
    Entity,
    EntityType,
    TimetableData,
    WeekNumber,
    Semester,
    ScheduleType,
    Lesson,
    DayName,
    LessonType,
    ScheduleForm,
    Subgroup,
)
from database import Database
from datetime import date, datetime, time
from difflib import SequenceMatcher
from profiler import profile
from loguru import logger


class SearchEntityBuilder:
    def __init__(self, database: Database):
        self._database = database
        self._search_text = None
        self._fuzzy = False

    def name(self, search_text: str) -> "SearchEntityBuilder":
        self._search_text = search_text
        return self

    def fuzzy(
        self,
        value: bool = True,
        search_threshold: float = 0.6,
        choose_threshold: float = 0.1,
    ) -> "SearchEntityBuilder":
        self._fuzzy = value
        self._search_threshold = search_threshold
        self._choose_threshold = 1.0 + choose_threshold
        return self

    @profile(func_name="search_entity_builder_fetch")
    async def fetch(self) -> Union[Optional[Entity], List[Entity]]:
        if not self._search_text:
            return [] if self._fuzzy else None

        if not self._fuzzy:
            timetable = await self._database.get_timetable_by_query(
                {"entity.name": self._search_text}
            )
            return timetable.entity if timetable else None

        all_entities = await self._database.get_all_entities()

        scored_entities = []
        for entity in all_entities:
            if not entity.name:
                continue

            similarity = SequenceMatcher(
                None, self._search_text.lower(), entity.name.lower()
            ).ratio()

            if similarity >= self._search_threshold:
                scored_entities.append((entity, similarity))

        scored_entities.sort(key=lambda x: x[1], reverse=True)

        logger.info(f"Found {len(scored_entities)} entities")

        if not scored_entities:
            return None

        if len(scored_entities) == 1:
            return scored_entities[0][0]

        if scored_entities[0][1] > scored_entities[1][1] * self._choose_threshold:
            return scored_entities[0][0]
        else:
            entities = [entity for entity, _ in scored_entities]

            if len(entities) > 7:
                return entities[:7]

            return entities


class SearchTimetableDataBuilder:
    def __init__(self, database: Database):
        self._database = database
        self._query = {}

    def entity_type(self, entity_type: EntityType) -> "SearchTimetableDataBuilder":
        self._query["entity.type"] = entity_type.value
        return self

    def entity_id(self, entity_id: int) -> "SearchTimetableDataBuilder":
        self._query["entity.id"] = entity_id
        return self

    def entity_name(self, name: str) -> "SearchTimetableDataBuilder":
        self._query["entity.name"] = name
        return self

    def years(self, years: str) -> "SearchTimetableDataBuilder":
        self._query["metadata.years"] = years
        return self

    def date(self, date_value: Union[date, datetime]) -> "SearchTimetableDataBuilder":
        if isinstance(date_value, datetime):
            date_value = date_value.date()
        self._query["metadata.date"] = date_value
        return self

    def week_number(self, week_number: WeekNumber) -> "SearchTimetableDataBuilder":
        self._query["metadata.week_number"] = week_number.value
        return self

    def semester(self, semester: Semester) -> "SearchTimetableDataBuilder":
        self._query["metadata.semester"] = semester.value
        return self

    def custom_query(self, query: Dict[str, Any]) -> "SearchTimetableDataBuilder":
        self._query.update(query)
        return self

    @profile(func_name="search_timetable_data_builder_fetch")
    async def fetch(self) -> Optional[TimetableData]:
        return await self._database.get_timetable_by_query(self._query)

    @profile(func_name="search_timetable_data_builder_fetch_all")
    async def fetch_all(self) -> List[TimetableData]:
        if not self._query:
            return await self._database.get_timetables()

        result = await self._database.get_timetable_by_query(self._query)
        return [result] if result else []


class FilteredLessonsBuilder:
    def __init__(self, lessons: List["Lesson"]):
        self._lessons = lessons
        self._filters = []

    def schedule_type(self, schedule_type: ScheduleType) -> "FilteredLessonsBuilder":
        self._filters.append(lambda lesson: lesson.schedule_type == schedule_type)
        return self

    def lesson_name(
        self, name: str, contains: bool = False
    ) -> "FilteredLessonsBuilder":
        if contains:
            self._filters.append(
                lambda lesson: name.lower() in lesson.lesson_name.lower()
            )
        else:
            self._filters.append(
                lambda lesson: lesson.lesson_name.lower() == name.lower()
            )
        return self

    def week_number(self, week_number: WeekNumber) -> "FilteredLessonsBuilder":
        self._filters.append(lambda lesson: lesson.week_number == week_number)
        return self

    def day_name(self, day_name: "DayName") -> "FilteredLessonsBuilder":
        self._filters.append(lambda lesson: lesson.day_name == day_name)
        return self

    def time_before(self, time_value: time) -> "FilteredLessonsBuilder":
        self._filters.append(lambda lesson: lesson.time_begin < time_value)
        return self

    def time_after(self, time_value: time) -> "FilteredLessonsBuilder":
        self._filters.append(lambda lesson: lesson.time_begin > time_value)
        return self

    def time_between(
        self, start_time: time, end_time: time
    ) -> "FilteredLessonsBuilder":
        self._filters.append(lambda lesson: start_time <= lesson.time_begin <= end_time)
        return self

    def date_before(self, date_value: date) -> "FilteredLessonsBuilder":
        self._filters.append(
            lambda lesson: lesson.day_date and lesson.day_date < date_value
        )
        return self

    def date_after(self, date_value: date) -> "FilteredLessonsBuilder":
        self._filters.append(
            lambda lesson: lesson.day_date and lesson.day_date > date_value
        )
        return self

    def date_between(
        self, start_date: date, end_date: date
    ) -> "FilteredLessonsBuilder":
        self._filters.append(
            lambda lesson: lesson.day_date and start_date <= lesson.day_date <= end_date
        )
        return self

    def date_equals(self, date_value: date) -> "FilteredLessonsBuilder":
        self._filters.append(
            lambda lesson: lesson.day_date and lesson.day_date == date_value
        )
        return self

    def has_group(
        self, group_name: str, exact: bool = False
    ) -> "FilteredLessonsBuilder":
        if exact:
            self._filters.append(
                lambda lesson: lesson.groups and group_name in lesson.groups
            )
        else:
            self._filters.append(
                lambda lesson: lesson.groups
                and any(group_name.lower() in group.lower() for group in lesson.groups)
            )
        return self

    def has_professor(
        self, professor_name: str, exact: bool = False
    ) -> "FilteredLessonsBuilder":
        if exact:
            self._filters.append(
                lambda lesson: lesson.professors and professor_name in lesson.professors
            )
        else:
            self._filters.append(
                lambda lesson: lesson.professors
                and any(
                    professor_name.lower() in prof.lower() for prof in lesson.professors
                )
            )
        return self

    def auditorium(
        self, auditorium: str, contains: bool = False
    ) -> "FilteredLessonsBuilder":
        if contains:
            self._filters.append(
                lambda lesson: lesson.auditorium
                and auditorium.lower() in lesson.auditorium.lower()
            )
        else:
            self._filters.append(
                lambda lesson: lesson.auditorium
                and lesson.auditorium.lower() == auditorium.lower()
            )
        return self

    def location(
        self, location: str, contains: bool = False
    ) -> "FilteredLessonsBuilder":
        if contains:
            self._filters.append(
                lambda lesson: lesson.location
                and location.lower() in lesson.location.lower()
            )
        else:
            self._filters.append(
                lambda lesson: lesson.location
                and lesson.location.lower() == location.lower()
            )
        return self

    def lesson_type(self, lesson_type: "LessonType") -> "FilteredLessonsBuilder":
        self._filters.append(lambda lesson: lesson.lesson_type == lesson_type)
        return self

    def schedule_form(self, schedule_form: "ScheduleForm") -> "FilteredLessonsBuilder":
        self._filters.append(lambda lesson: lesson.schedule_form == schedule_form)
        return self

    def subgroup(self, subgroup: "Subgroup") -> "FilteredLessonsBuilder":
        self._filters.append(
            lambda lesson: lesson.subgroups == subgroup
            or lesson.subgroups == Subgroup.COMMON
        )
        return self

    def custom_filter(
        self, predicate: Callable[["Lesson"], bool]
    ) -> "FilteredLessonsBuilder":
        self._filters.append(predicate)
        return self

    @profile(func_name="filtered_lessons_builder_build")
    def build(self) -> List["Lesson"]:
        result = self._lessons

        for filter_func in self._filters:
            result = [lesson for lesson in result if filter_func(lesson)]

        return result
