from dataclasses import dataclass
from typing import Optional, List, Any
from enum import Enum
from datetime import date, time, timedelta


class EntityType(Enum):
    GROUP = "group"
    PROFESSOR = "professor"
    AUDITORIUM = "auditorium"


class Semester(Enum):
    FIRST = "1 семестр"
    SECOND = "2 семестр"


class WeekNumber(Enum):
    ODD = "1 неделя"
    EVEN = "2 неделя"


class ScheduleType(Enum):
    REGULAR = "Расписание занятий"
    SESSION = "Расписание сессии"
    CONSULTATION = "Расписание консультаций"


class ScheduleForm(Enum):
    OFFLINE = "Очная форма обучения"
    ONLINE = "Очно-заочная форма обучения"
    DISTANCE = "Заочная форма обучения"


class DayName(Enum):
    MONDAY = "Понедельник"
    TUESDAY = "Вторник"
    WEDNESDAY = "Среда"
    THURSDAY = "Четверг"
    FRIDAY = "Пятница"
    SATURDAY = "Суббота"
    SUNDAY = "Воскресенье"


class LessonType(Enum):
    LECTURE = "Лекция"
    PRACTICE = "Практика"
    LABORATORY = "Лабораторная работа"
    CONSULTATION = "Консультация"
    EXAM = "Экзамен"


class Subgroup(Enum):
    FIRST = "1 подгруппа"
    SECOND = "2 подгруппа"
    COMMON = ""


@dataclass
class Lesson:
    schedule_type: ScheduleType
    time_begin: time
    lesson_name: str  # reformat "Математический анализ" (только первая буква большая)
    schedule_form: Optional[ScheduleForm] = None
    week_number: Optional[WeekNumber] = None
    day_name: Optional[DayName] = None
    day_date: Optional[date] = None  # пока не используется
    duration: Optional[timedelta] = None
    lesson_type: Optional[LessonType] = None
    groups: Optional[list[str]] = None  # "БПИ23-01"
    professors: Optional[list[str]] = None  # "Алиева Д. П."
    auditorium: Optional[str] = None  # reformat "Л-307" (отсечен адресс)
    location: Optional[str] = None  # "пр. им. газеты Красноярский рабочий, 31"
    subgroups: Subgroup = Subgroup.COMMON


@dataclass
class Entity:
    type: EntityType
    id: int
    name: Optional[str] = None  # "БПИ23-01" or "Алиева Д. П." or "Л-307"


@dataclass
class Metadata:
    years: str  # "2024-2025"
    date: date  # "26.03.2025"
    week_number: WeekNumber
    semester: Optional[Semester] = None


@dataclass
class TimetableData:
    entity: Entity
    metadata: Metadata
    lessons: list[Lesson]


class ChangeType(Enum):
    METADATA = "metadata"
    LESSON_ADDED = "lesson_added"
    LESSON_REMOVED = "lesson_removed"
    LESSON_MODIFIED = "lesson_modified"


@dataclass
class FieldChange:
    field_name: str
    old_value: Any
    new_value: Any


@dataclass
class LessonChange:
    change_type: ChangeType
    field_changes: List[FieldChange]


@dataclass
class TimetableChangeData:
    entity: Entity
    metadata_changes: Optional[List[FieldChange]] = None
    lesson_changes: Optional[List[LessonChange]] = None
