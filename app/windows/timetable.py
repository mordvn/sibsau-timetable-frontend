from aiogram_dialog.widgets.text import Const, Format
from aiogram_dialog.widgets.kbd import Button, Row
from aiogram_dialog import Window, DialogManager
from windows.states import BotStates
from parser_types import WeekNumber, DayName, ScheduleType, Subgroup
from datetime import datetime, timedelta
from parser_types import TimetableData
from database import database
from database_searcher import SearchTimetableDataBuilder, FilteredLessonsBuilder
from aiogram.enums import ParseMode
from parser_types import Entity
from aiogram.utils.deep_linking import create_start_link
from aiogram import Bot
from profiler import profile


@profile(func_name="timetable_get_timetable_data")
async def _get_timetable_data(entity: Entity, dialog_manager: DialogManager):
    timetable_data: TimetableData = dialog_manager.dialog_data.get("timetable_data")
    if not timetable_data:
        timetable_data = (
            await SearchTimetableDataBuilder(database).entity_id(entity.id).fetch()
        )
        dialog_manager.dialog_data["timetable_data"] = timetable_data

    return timetable_data


@profile(func_name="timetable_get_relative_day_info")
def _get_relative_day_info(offset=0):
    today = datetime.now()
    target_date = today + timedelta(days=offset)
    target_weekday = target_date.weekday()

    day_names = [
        DayName.MONDAY,
        DayName.TUESDAY,
        DayName.WEDNESDAY,
        DayName.THURSDAY,
        DayName.FRIDAY,
        DayName.SATURDAY,
        DayName.SUNDAY,
    ]
    day_name = day_names[target_weekday]

    suffix = "Сегодня"
    if offset == 1:
        suffix = "Завтра"
    elif offset == -1:
        suffix = "Вчера"
    elif offset == 2:
        suffix = "Послезавтра"
    elif offset == -2:
        suffix = "Позавчера"
    elif offset > 0:
        suffix = f"Через {offset} дн."
    elif offset < 0:
        suffix = f"{abs(offset)} дн. назад"

    return day_name, suffix


def _get_current_week_number():
    today = datetime.now()
    return WeekNumber.EVEN if today.isocalendar()[1] % 2 != 0 else WeekNumber.ODD


@profile(func_name="timetable_get_day_offset_from_today")
def _get_day_offset_from_today(day_name, selected_week_number):
    today = datetime.now()
    today_weekday = today.weekday()

    day_names = [
        DayName.MONDAY,
        DayName.TUESDAY,
        DayName.WEDNESDAY,
        DayName.THURSDAY,
        DayName.FRIDAY,
        DayName.SATURDAY,
        DayName.SUNDAY,
    ]
    day_index = day_names.index(day_name)

    current_week = _get_current_week_number()

    offset = day_index - today_weekday

    if selected_week_number != current_week:
        if offset > 0:
            offset += 7
        else:
            offset += 14

    return offset


@profile(func_name="timetable_get_quick_link")
async def _get_quick_link(bot: Bot, entity_name: str):
    return await create_start_link(bot, f"{entity_name}", encode=True)


def _html_wrap_bold(text):
    return f"<b>{text}</b>"


def _html_wrap_link(text, url):
    return f"<a href='{url}'>{text}</a>"


@profile(func_name="timetable_getter")
async def timetable_getter(dialog_manager: DialogManager, **kwargs):
    entity: Entity = dialog_manager.start_data.get("entity")
    timetable_data = await _get_timetable_data(entity, dialog_manager)
    bot = dialog_manager.middleware_data.get("bot_instance")

    user_id = dialog_manager.event.from_user.id

    current_week = _get_current_week_number()
    if "is_first_open" not in dialog_manager.dialog_data:
        day_name, suffix = _get_relative_day_info(0)
        dialog_manager.dialog_data["filter_day_name"] = day_name
        dialog_manager.dialog_data["filter_day_suffix"] = "(" + suffix + ")"

        current_week = _get_current_week_number()
        dialog_manager.dialog_data["filter_week_number"] = current_week

        dialog_manager.dialog_data["is_first_open"] = False

        if (
            timetable_data
            and timetable_data.metadata
            and timetable_data.metadata.week_number
        ):
            dialog_manager.dialog_data["filter_week_number"] = (
                timetable_data.metadata.week_number
            )
        else:
            dialog_manager.dialog_data["filter_week_number"] = current_week

        if (
            user_id
            and timetable_data
            and timetable_data.entity
            and timetable_data.entity.name
        ):
            try:
                dialog_manager.dialog_data[
                    "is_subscribed"
                ] = await database.user_is_subscribed(
                    user_id, timetable_data.entity.name
                )

                # Загружаем сохраненную подгруппу
                saved_subgroup = await database.get_user_subgroup(
                    user_id, timetable_data.entity.name
                )
                dialog_manager.dialog_data["filter_subgroup"] = saved_subgroup
            except Exception:
                dialog_manager.dialog_data["is_subscribed"] = False
                dialog_manager.dialog_data["filter_subgroup"] = Subgroup.COMMON

    dialog_manager.dialog_data["filter_week_number"] = dialog_manager.dialog_data.get(
        "filter_week_number", current_week
    )
    dialog_manager.dialog_data["filter_day_name"] = dialog_manager.dialog_data.get(
        "filter_day_name", DayName.MONDAY
    )
    dialog_manager.dialog_data["filter_day_suffix"] = dialog_manager.dialog_data.get(
        "filter_day_suffix", "Сегодня"
    )
    dialog_manager.dialog_data["filter_schedule_type"] = dialog_manager.dialog_data.get(
        "filter_schedule_type", ScheduleType.REGULAR
    )
    dialog_manager.dialog_data["filter_subgroup"] = dialog_manager.dialog_data.get(
        "filter_subgroup", Subgroup.COMMON
    )
    dialog_manager.dialog_data["is_subscribed"] = dialog_manager.dialog_data.get(
        "is_subscribed", False
    )

    selected_day = dialog_manager.dialog_data["filter_day_name"]
    selected_week = dialog_manager.dialog_data["filter_week_number"]

    offset = _get_day_offset_from_today(selected_day, selected_week)

    if -5 <= offset <= 5:
        _, suffix = _get_relative_day_info(offset)
        dialog_manager.dialog_data["filter_day_suffix"] = "(" + suffix + ")"
    else:
        dialog_manager.dialog_data["filter_day_suffix"] = ""

    day_names = [
        DayName.MONDAY,
        DayName.TUESDAY,
        DayName.WEDNESDAY,
        DayName.THURSDAY,
        DayName.FRIDAY,
        DayName.SATURDAY,
        DayName.SUNDAY,
    ]
    day_index = day_names.index(dialog_manager.dialog_data["filter_day_name"]) + 1
    dialog_manager.dialog_data["day_index"] = day_index

    builder = FilteredLessonsBuilder(timetable_data.lessons)
    builder = (
        builder.week_number(dialog_manager.dialog_data["filter_week_number"])
        .day_name(dialog_manager.dialog_data["filter_day_name"])
        .schedule_type(dialog_manager.dialog_data["filter_schedule_type"])
    )

    current_subgroup = dialog_manager.dialog_data["filter_subgroup"]
    if current_subgroup == Subgroup.COMMON:
        pass
    else:
        builder = builder.subgroup(current_subgroup)

    filtered_lessons = builder.build()

    formatted_lessons = await format_lessons(filtered_lessons, bot)

    week_text = f"{1 if dialog_manager.dialog_data['filter_week_number'] == WeekNumber.ODD else 2}/2"
    day_text = f"{day_index}/7"

    if dialog_manager.dialog_data["filter_subgroup"] == Subgroup.COMMON:
        subgroup_text = "_"
    elif dialog_manager.dialog_data["filter_subgroup"] == Subgroup.FIRST:
        subgroup_text = "1 подгруппа"
    else:
        subgroup_text = "2 подгруппа"

    subscribe_icon = "🔔" if not dialog_manager.dialog_data["is_subscribed"] else "🔕"

    tab_regular_text = (
        ".."
        if dialog_manager.dialog_data["filter_schedule_type"] == ScheduleType.REGULAR
        else "Основное"
    )
    tab_consultations_text = (
        ".."
        if dialog_manager.dialog_data["filter_schedule_type"]
        == ScheduleType.CONSULTATION
        else "Консультации"
    )
    tab_session_text = (
        ".."
        if dialog_manager.dialog_data["filter_schedule_type"] == ScheduleType.SESSION
        else "Сессия"
    )

    return {
        "timetable_data": timetable_data,
        "filtered_lessons": formatted_lessons,
        "week_text": week_text,
        "day_text": day_text,
        "subgroup_text": subgroup_text,
        "subscribe_icon": subscribe_icon,
        "tab_regular_text": tab_regular_text,
        "tab_consultations_text": tab_consultations_text,
        "tab_session_text": tab_session_text,
        "main_entity_link": await _get_quick_link(bot, timetable_data.entity.name),
        "optional_semester": timetable_data.metadata.semester.value if timetable_data.metadata.semester else "",
    }


@profile(func_name="timetable_format_lessons")
async def format_lessons(lessons, bot=None):
    if not lessons:
        return "Занятия не найдены для выбранных фильтров"

    lesson_times = [
        ("08:00", "09:30", "1"),
        ("09:40", "11:10", "2"),
        ("11:30", "13:00", "3"),
        ("13:30", "15:00", "4"),
        ("15:10", "16:40", "5"),
        ("16:50", "18:20", "6"),
        ("18:30", "20:00", "7"),
        ("20:10", "21:40", "8"),
    ]

    result = ""
    for i, lesson in enumerate(lessons):
        result += f"<b>{lesson.lesson_name}</b>\n"

        time_str = lesson.time_begin.strftime("%H:%M") if lesson.time_begin else "??:??"

        lesson_number = ""
        for idx, (start, end, num) in enumerate(lesson_times):
            if time_str == start:
                lesson_number = num
                break

        if lesson_number:
            cube_emoji = {
                "1": "1️⃣",
                "2": "2️⃣",
                "3": "3️⃣",
                "4": "4️⃣",
                "5": "5️⃣",
                "6": "6️⃣",
                "7": "7️⃣",
                "8": "8️⃣",
            }.get(lesson_number, "🔹")
            result += f"{cube_emoji} "
        else:
            result += "🔹 "

        duration = lesson.duration.total_seconds() // 60 if lesson.duration else 90
        end_time = (
            (
                datetime.combine(datetime.today(), lesson.time_begin)
                + timedelta(minutes=duration)
            ).time()
            if lesson.time_begin
            else None
        )
        end_time_str = end_time.strftime("%H:%M") if end_time else "??:??"

        result += f"<b>{time_str}-{end_time_str}</b>"
        if lesson.lesson_type:
            result += f" | {lesson.lesson_type.value}"
        if lesson.subgroups and lesson.subgroups != Subgroup.COMMON:
            result += f" | {lesson.subgroups.value}"
        result += "\n"

        location = []
        if lesson.auditorium:
            if bot:
                link = await _get_quick_link(bot, lesson.auditorium)
                location.append(_html_wrap_link(lesson.auditorium, link))

        if location:
            twogis_link_sign = f'<a href="https://2gis.ru/krasnoyarsk/search/{lesson.location}">📍</a>'
            result += f"{' '.join(location)} {twogis_link_sign} \n"

        if lesson.professors and len(lesson.professors) > 0:
            professors_links = []
            for professor in lesson.professors:
                if bot:
                    link = await _get_quick_link(bot, professor)
                    professors_links.append(_html_wrap_link(professor, link))
            result += f"{', '.join(professors_links)}\n"

        if lesson.groups and len(lesson.groups) > 0:
            groups_links = []
            for group in lesson.groups:
                if bot:
                    link = await _get_quick_link(bot, group)
                    groups_links.append(_html_wrap_link(group, link))
            result += f"{', '.join(groups_links)}\n"

        result += "\n"

    return result


@profile(func_name="timetable_subscribe_click")
async def subscribe_click(callback, widget, manager: DialogManager, **kwargs):
    new_subscription_state = not manager.dialog_data.get("is_subscribed", False)
    manager.dialog_data["is_subscribed"] = new_subscription_state

    user_id = callback.from_user.id
    entity_name = manager.dialog_data.get("timetable_data").entity.name

    success = False
    if new_subscription_state:
        success = await database.user_subscribe(user_id, entity_name)
    else:
        success = await database.user_unsubscribe(user_id, entity_name)

    if success:
        is_subscribed = await database.user_is_subscribed(user_id, entity_name)
        manager.dialog_data["is_subscribed"] = is_subscribed

        await callback.answer(
            "Отслеживание расписания включено"
            if is_subscribed
            else "Отслеживание расписания выключено",
            show_alert=True,
        )
    else:
        manager.dialog_data["is_subscribed"] = not new_subscription_state
        await callback.answer(
            "Не удалось изменить статус подписки. Попробуйте позже.",
            show_alert=True,
        )


@profile(func_name="timetable_tab_regular_click")
async def tab_regular_click(callback, widget, manager: DialogManager, **kwargs):
    manager.dialog_data["filter_schedule_type"] = ScheduleType.REGULAR


@profile(func_name="timetable_tab_consultations_click")
async def tab_consultations_click(callback, widget, manager: DialogManager, **kwargs):
    manager.dialog_data["filter_schedule_type"] = ScheduleType.CONSULTATION


@profile(func_name="timetable_tab_session_click")
async def tab_session_click(callback, widget, manager: DialogManager, **kwargs):
    manager.dialog_data["filter_schedule_type"] = ScheduleType.SESSION


async def week_switch_click(callback, widget, manager: DialogManager, **kwargs):
    current_week = manager.dialog_data.get("filter_week_number", WeekNumber.ODD)
    manager.dialog_data["filter_week_number"] = (
        WeekNumber.EVEN if current_week == WeekNumber.ODD else WeekNumber.ODD
    )


@profile(func_name="timetable_week_switch_click")
async def day_prev_click(callback, widget, manager: DialogManager, **kwargs):
    day_names = [
        DayName.MONDAY,
        DayName.TUESDAY,
        DayName.WEDNESDAY,
        DayName.THURSDAY,
        DayName.FRIDAY,
        DayName.SATURDAY,
        DayName.SUNDAY,
    ]

    current_day = manager.dialog_data.get("filter_day_name", DayName.MONDAY)
    current_index = day_names.index(current_day)

    if current_index == 0:
        current_week = manager.dialog_data.get("filter_week_number", WeekNumber.ODD)
        manager.dialog_data["filter_week_number"] = (
            WeekNumber.EVEN if current_week == WeekNumber.ODD else WeekNumber.ODD
        )
        prev_index = 6
    else:
        prev_index = current_index - 1

    manager.dialog_data["filter_day_name"] = day_names[prev_index]

    offset = _get_day_offset_from_today(
        manager.dialog_data["filter_day_name"],
        manager.dialog_data["filter_week_number"],
    )

    if -5 <= offset <= 5:
        _, suffix = _get_relative_day_info(offset)
        manager.dialog_data["filter_day_suffix"] = "(" + suffix + ")"
    else:
        manager.dialog_data["filter_day_suffix"] = ""


@profile(func_name="timetable_day_today_click")
async def day_today_click(callback, widget, manager: DialogManager, **kwargs):
    day_name, suffix = _get_relative_day_info(0)
    manager.dialog_data["filter_day_name"] = day_name
    manager.dialog_data["filter_day_suffix"] = "(" + suffix + ")"

    today = datetime.now()
    current_week = (
        WeekNumber.EVEN if today.isocalendar()[1] % 2 == 0 else WeekNumber.ODD
    )
    manager.dialog_data["filter_week_number"] = current_week


@profile(func_name="timetable_day_next_click")
async def day_next_click(callback, widget, manager: DialogManager, **kwargs):
    day_names = [
        DayName.MONDAY,
        DayName.TUESDAY,
        DayName.WEDNESDAY,
        DayName.THURSDAY,
        DayName.FRIDAY,
        DayName.SATURDAY,
        DayName.SUNDAY,
    ]

    current_day = manager.dialog_data.get("filter_day_name", DayName.MONDAY)
    current_index = day_names.index(current_day)

    if current_index == 6:
        current_week = manager.dialog_data.get("filter_week_number", WeekNumber.ODD)
        manager.dialog_data["filter_week_number"] = (
            WeekNumber.EVEN if current_week == WeekNumber.ODD else WeekNumber.ODD
        )
        next_index = 0
    else:
        next_index = current_index + 1

    manager.dialog_data["filter_day_name"] = day_names[next_index]

    offset = _get_day_offset_from_today(
        manager.dialog_data["filter_day_name"],
        manager.dialog_data["filter_week_number"],
    )

    if -5 <= offset <= 5:
        _, suffix = _get_relative_day_info(offset)
        manager.dialog_data["filter_day_suffix"] = "(" + suffix + ")"
    else:
        manager.dialog_data["filter_day_suffix"] = ""


@profile(func_name="timetable_subgroup_switch_click")
async def subgroup_switch_click(callback, widget, manager: DialogManager, **kwargs):
    current_subgroup = manager.dialog_data.get("filter_subgroup", Subgroup.COMMON)
    subgroups = [Subgroup.COMMON, Subgroup.FIRST, Subgroup.SECOND]
    current_index = subgroups.index(current_subgroup)
    next_index = (current_index + 1) % len(subgroups)
    new_subgroup = subgroups[next_index]
    manager.dialog_data["filter_subgroup"] = new_subgroup

    # Сохраняем настройку пользователя в базе данных
    user_id = callback.from_user.id
    entity_name = manager.dialog_data.get("timetable_data").entity.name

    await database.save_user_subgroup(user_id, entity_name, new_subgroup)


timetable_window = Window(
    Format(
        "<b><a href='{main_entity_link}'>{timetable_data.entity.name}</a> | {optional_semester} {timetable_data.metadata.years}</b>"
    ),
    Format(
        "<b>{dialog_data[filter_week_number].value} | {dialog_data[filter_day_name].value} {dialog_data[filter_day_suffix]}</b>"
    ),
    Format("\n{filtered_lessons}"),
    Row(
        Button(
            Format("{tab_regular_text}"),
            id="tab_regular",
            on_click=tab_regular_click,
        ),
        Button(
            Format("{tab_consultations_text}"),
            id="tab_consultations",
            on_click=tab_consultations_click,
        ),
        Button(
            Format("{tab_session_text}"),
            id="tab_session",
            on_click=tab_session_click,
        ),
    ),
    Row(
        Button(
            Format("{week_text}"),
            id="week_switch",
            on_click=week_switch_click,
        ),
        Button(
            Const("<<"),
            id="day_prev",
            on_click=day_prev_click,
        ),
        Button(
            Format("{day_text}"),
            id="day_today",
            on_click=day_today_click,
        ),
        Button(
            Const(">>"),
            id="day_next",
            on_click=day_next_click,
        ),
    ),
    Row(
        Button(
            Format("{subscribe_icon}"),
            id="subscribe",
            on_click=subscribe_click,
        ),
        Button(
            Format("{subgroup_text}"),
            id="subgroup_switch",
            on_click=subgroup_switch_click,
        ),
    ),
    state=BotStates.timetable,
    getter=timetable_getter,
    parse_mode=ParseMode.HTML,
)
