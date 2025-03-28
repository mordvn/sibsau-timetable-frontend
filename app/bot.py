from aiogram import Bot, Dispatcher
from aiogram.filters import Command, CommandStart, CommandObject
from aiogram.utils.deep_linking import decode_payload
from aiogram.types import Message
from aiogram.enums.parse_mode import ParseMode
from aiogram.fsm.storage.memory import MemoryStorage
from profiler import profile
from aiogram_dialog import (
    Dialog,
    DialogManager,
    setup_dialogs,
    StartMode,
)
from parser_types import (
    TimetableChangeData, 
    ChangeType,
    Lesson,
)
from windows.states import BotStates
from loguru import logger
import logging
import sys
from config import settings
from windows import tutorial_window, wait_for_entity_choose_window, timetable_window
from database_searcher import SearchEntityBuilder
from database import database
from var_dump import var_dump
from aiogram.utils.chat_action import ChatActionSender
from aiogram.utils.deep_linking import create_start_link

dialog = Dialog(tutorial_window, wait_for_entity_choose_window, timetable_window)


storage = MemoryStorage()


class BotRunner:
    bot = None
    dp = None

    @staticmethod
    @profile(func_name="bot_runner_init")
    async def init(token: str):
        BotRunner.bot = Bot(token=token)
        BotRunner.dp = Dispatcher(storage=storage)

        BotRunner.dp["bot_instance"] = BotRunner.bot
        BotRunner.dp.include_router(dialog)
        setup_dialogs(BotRunner.dp)

        BotRunner.dp.message.register(BotRunner._process_help, Command("help"))
        BotRunner.dp.message.register(
            BotRunner._process_start_with_deep_link, CommandStart(deep_link="start")
        )
        BotRunner.dp.message.register(BotRunner._process_start, CommandStart())
        BotRunner.dp.message.register(BotRunner._process_message)

        logger.debug("Инициализация бота прошла успешно")

    @staticmethod
    async def run_bot():
        logging.basicConfig(level=logging.INFO, stream=sys.stdout)
        await BotRunner.dp.start_polling(BotRunner.bot, skip_updates=True)

    @staticmethod
    async def _format_lesson(lesson: Lesson, bot: Bot = None) -> str:
        result = ""
        
        name = lesson.lesson_name
        
        if lesson.day_date:
            date_str = lesson.day_date.strftime("%d.%m.%Y")
            time_str = lesson.time_begin.strftime("%H:%M") if lesson.time_begin else ""
            if time_str:
                result += f"<b>{date_str}, {time_str} - {name}</b>\n"
            else:
                result += f"<b>{date_str} - {name}</b>\n"
        else:
            week = lesson.week_number.value if lesson.week_number else ""
            day = lesson.day_name.value if lesson.day_name else ""
            time_str = lesson.time_begin.strftime("%H:%M") if lesson.time_begin else ""
            
            header_parts = []
            if week:
                header_parts.append(week)
            if day:
                header_parts.append(day)
            if time_str:
                header_parts.append(time_str)
                
            if header_parts:
                result += f"<b>{', '.join(header_parts)} - {name}</b>\n"
            else:
                result += f"<b>{name}</b>\n"
        
        if lesson.lesson_type:
            result += f"Тип: {lesson.lesson_type.value}\n"
        
        if lesson.auditorium:
            if bot:
                link = await BotRunner._get_quick_link(bot, lesson.auditorium)
                result += f"Аудитория: <a href='{link}'>{lesson.auditorium}</a>\n"
            else:
                result += f"Аудитория: {lesson.auditorium}\n"
        
        if lesson.professors:
            if bot:
                profs_links = []
                for professor in lesson.professors:
                    link = await BotRunner._get_quick_link(bot, professor)
                    profs_links.append(f"<a href='{link}'>{professor}</a>")
                result += f"Преподаватели: {', '.join(profs_links)}\n"
            else:
                result += f"Преподаватели: {', '.join(lesson.professors)}\n"
        
        if hasattr(lesson, 'subgroups') and lesson.subgroups and lesson.subgroups.value:
            result += f"Подгруппа: {lesson.subgroups.value}\n"
        
        if lesson.groups and bot:
            groups_links = []
            for group in lesson.groups:
                link = await BotRunner._get_quick_link(bot, group)
                groups_links.append(f"<a href='{link}'>{group}</a>")
            if groups_links:
                result += f"Группы: {', '.join(groups_links)}\n"
        
        return result
    
    @staticmethod
    async def _get_quick_link(bot: Bot, entity_name: str):
        return await create_start_link(bot, f"{entity_name}", encode=True)
    
    @staticmethod
    @profile(func_name="bot_runner_receive_notification")
    async def receive_notification(message: TimetableChangeData):
        if not message.lesson_changes:
            return
            
        users = await database.get_subscribed_users(message.entity.name)
        if not users:
            return
            
        link = await BotRunner._get_quick_link(BotRunner.bot, message.entity.name)
        text = f"🔔 <b>Изменения в расписании: <a href='{link}'>{message.entity.name}</a></b>\n\n"
        
        for change in message.lesson_changes:
            if change.change_type == ChangeType.LESSON_ADDED:
                text += f"<b>➕ Добавлено занятие:</b>\n"
                text += await BotRunner._format_lesson(change.new_lesson, BotRunner.bot)
                text += "\n"
            elif change.change_type == ChangeType.LESSON_REMOVED:
                text += f"<b>➖ Удалено занятие:</b>\n"
                text += await BotRunner._format_lesson(change.old_lesson, BotRunner.bot)
                text += "\n"
            else:  # LESSON_MODIFIED
                text += f"<b>🔄 Изменено занятие:</b>\n"
                
                old_lesson = change.old_lesson
                new_lesson = change.new_lesson
                
                if new_lesson.day_date:
                    date_str = new_lesson.day_date.strftime("%d.%m.%Y")
                    time_str = new_lesson.time_begin.strftime("%H:%M") if new_lesson.time_begin else ""
                    if time_str:
                        text += f"<b>{date_str}, {time_str} - {new_lesson.lesson_name}</b>\n"
                    else:
                        text += f"<b>{date_str} - {new_lesson.lesson_name}</b>\n"
                else:
                    week = new_lesson.week_number.value if new_lesson.week_number else ""
                    day = new_lesson.day_name.value if new_lesson.day_name else ""
                    time_str = new_lesson.time_begin.strftime("%H:%M") if new_lesson.time_begin else ""
                    
                    header_parts = []
                    if week:
                        header_parts.append(week)
                    if day:
                        header_parts.append(day)
                    if time_str:
                        header_parts.append(time_str)
                        
                    if header_parts:
                        text += f"<b>{', '.join(header_parts)} - {new_lesson.lesson_name}</b>\n"
                    else:
                        text += f"<b>{new_lesson.lesson_name}</b>\n"
                
                changes = []
                
                old_aud = old_lesson.auditorium
                new_aud = new_lesson.auditorium
                if old_aud != new_aud:
                    if BotRunner.bot:
                        old_link = await BotRunner._get_quick_link(BotRunner.bot, old_aud) if old_aud else ""
                        new_link = await BotRunner._get_quick_link(BotRunner.bot, new_aud) if new_aud else ""
                        
                        old_text = f"<a href='{old_link}'>{old_aud}</a>" if old_aud else ""
                        new_text = f"<a href='{new_link}'>{new_aud}</a>" if new_aud else ""
                        
                        changes.append(f"Аудитория: <s>{old_text}</s> → <b>{new_text}</b>")
                    else:
                        changes.append(f"Аудитория: <s>{old_aud}</s> → <b>{new_aud}</b>")
                
                old_type = old_lesson.lesson_type.value if old_lesson.lesson_type else ""
                new_type = new_lesson.lesson_type.value if new_lesson.lesson_type else ""
                if old_type != new_type and old_type and new_type:
                    changes.append(f"Тип: <s>{old_type}</s> → <b>{new_type}</b>")
                
                old_profs = old_lesson.professors if old_lesson.professors else []
                new_profs = new_lesson.professors if new_lesson.professors else []
                
                if str(old_profs) != str(new_profs):
                    if BotRunner.bot:
                        old_links = []
                        for prof in old_profs:
                            link = await BotRunner._get_quick_link(BotRunner.bot, prof)
                            old_links.append(f"<a href='{link}'>{prof}</a>")
                        
                        new_links = []
                        for prof in new_profs:
                            link = await BotRunner._get_quick_link(BotRunner.bot, prof)
                            new_links.append(f"<a href='{link}'>{prof}</a>")
                            
                        old_text = ", ".join(old_links) if old_links else ""
                        new_text = ", ".join(new_links) if new_links else ""
                        
                        if old_text and new_text:
                            changes.append(f"Преподаватели: <s>{old_text}</s> → <b>{new_text}</b>")
                    else:
                        old_text = ", ".join(old_profs) if old_profs else ""
                        new_text = ", ".join(new_profs) if new_profs else ""
                        if old_text and new_text:
                            changes.append(f"Преподаватели: <s>{old_text}</s> → <b>{new_text}</b>")
                
                if hasattr(old_lesson, 'subgroups') and hasattr(new_lesson, 'subgroups'):
                    old_subgroup = old_lesson.subgroups.value if old_lesson.subgroups else ""
                    new_subgroup = new_lesson.subgroups.value if new_lesson.subgroups else ""
                    if old_subgroup != new_subgroup and old_subgroup and new_subgroup:
                        changes.append(f"Подгруппа: <s>{old_subgroup}</s> → <b>{new_subgroup}</b>")
                
                if old_lesson.day_date and new_lesson.day_date and old_lesson.day_date != new_lesson.day_date:
                    old_date = old_lesson.day_date.strftime("%d.%m.%Y")
                    new_date = new_lesson.day_date.strftime("%d.%m.%Y")
                    changes.append(f"Дата: <s>{old_date}</s> → <b>{new_date}</b>")
                
                old_time = old_lesson.time_begin.strftime("%H:%M") if old_lesson.time_begin else ""
                new_time = new_lesson.time_begin.strftime("%H:%M") if new_lesson.time_begin else ""
                if old_time != new_time and old_time and new_time:
                    changes.append(f"Время: <s>{old_time}</s> → <b>{new_time}</b>")
                
                if not old_lesson.day_date and not new_lesson.day_date:
                    old_day = old_lesson.day_name.value if old_lesson.day_name else ""
                    new_day = new_lesson.day_name.value if new_lesson.day_name else ""
                    if old_day != new_day and old_day and new_day:
                        changes.append(f"День: <s>{old_day}</s> → <b>{new_day}</b>")
                
                if not old_lesson.day_date and not new_lesson.day_date:
                    old_week = old_lesson.week_number.value if old_lesson.week_number else ""
                    new_week = new_lesson.week_number.value if new_lesson.week_number else ""
                    if old_week != new_week and old_week and new_week:
                        changes.append(f"Неделя: <s>{old_week}</s> → <b>{new_week}</b>")
                
                if old_lesson.lesson_name != new_lesson.lesson_name:
                    changes.append(f"Название: <s>{old_lesson.lesson_name}</s> → <b>{new_lesson.lesson_name}</b>")
                
                if old_lesson.groups != new_lesson.groups and BotRunner.bot:
                    old_groups = old_lesson.groups if old_lesson.groups else []
                    new_groups = new_lesson.groups if new_lesson.groups else []
                    
                    old_links = []
                    for group in old_groups:
                        link = await BotRunner._get_quick_link(BotRunner.bot, group)
                        old_links.append(f"<a href='{link}'>{group}</a>")
                    
                    new_links = []
                    for group in new_groups:
                        link = await BotRunner._get_quick_link(BotRunner.bot, group)
                        new_links.append(f"<a href='{link}'>{group}</a>")
                        
                    old_text = ", ".join(old_links) if old_links else ""
                    new_text = ", ".join(new_links) if new_links else ""
                    
                    if old_text and new_text:
                        changes.append(f"Группы: <s>{old_text}</s> → <b>{new_text}</b>")
                
                if changes:
                    text += "Изменения:\n"
                    for change_info in changes:
                        text += f"• {change_info}\n"
                else:
                    text += "<i>Нет значимых изменений</i>\n"
                    
                text += "\n"

        if len(text) > 4000:
            text = text[:3950] + "...\n\n<i>Сообщение слишком длинное</i>"
        
        for user in users:
            try:
                await BotRunner.bot.send_message(
                    user, text, parse_mode=ParseMode.HTML
                )
            except Exception as e:
                var_dump(f"Ошибка отправки: {e}")
                
    @staticmethod
    @profile(func_name="bot_runner_process_start")
    async def _process_start(message: Message, dialog_manager: DialogManager):
        await message.answer(
            "Напиши название группы, фамилию преподавателя или аудиторию, как ты делал(а) это на сайте"
        )

    @staticmethod
    @profile(func_name="bot_runner_process_help")
    async def _process_help(message: Message, dialog_manager: DialogManager):
        await dialog_manager.start(BotStates.tutorial, mode=StartMode.NEW_STACK)

    @staticmethod
    @profile(func_name="bot_runner_receive_timetable_search_request")
    async def _receive_timetable_search_request(
        message: Message, dialog_manager: DialogManager, request: str
    ):
        builder = SearchEntityBuilder(database)
        search_results = await builder.name(request).fetch()

        if search_results:
            if isinstance(search_results, list):
                if len(search_results) == 1:
                    await dialog_manager.start(
                        BotStates.timetable,
                        data={"entity": search_results[0]},
                        mode=StartMode.NEW_STACK,
                    )
                else:
                    await dialog_manager.start(
                        BotStates.wait_for_entity_choose,
                        data={"entities": search_results},
                        mode=StartMode.NEW_STACK,
                    )
            else:
                await dialog_manager.start(
                    BotStates.timetable,
                    data={"entity": search_results},
                    mode=StartMode.NEW_STACK,
                )
        else:
            async with ChatActionSender.typing(
                bot=BotRunner.bot, chat_id=message.chat.id
            ):
                search_results = (
                    await builder.name(request).fuzzy(True, 0.5, 0.05).fetch()
                )

                if search_results:
                    if isinstance(search_results, list):
                        await dialog_manager.start(
                            BotStates.wait_for_entity_choose,
                            data={"entities": search_results},
                            mode=StartMode.NEW_STACK,
                        )
                    else:
                        await dialog_manager.start(
                            BotStates.timetable,
                            data={"entity": search_results},
                            mode=StartMode.NEW_STACK,
                        )
                else:
                    await message.answer(
                        "Ничего не найдено по вашему запросу. Попробуйте другой запрос."
                    )

    @staticmethod
    @profile(func_name="bot_runner_process_start_with_deep_link")
    async def _process_start_with_deep_link(
        message: Message, dialog_manager: DialogManager, command: CommandObject
    ):
        if command.args:
            payload = decode_payload(command.args)
            await BotRunner._receive_timetable_search_request(
                message, dialog_manager, payload
            )
        else:
            await message.answer("Не удалось распознать ссылку")

    @staticmethod
    @profile(func_name="bot_runner_process_message")
    async def _process_message(message: Message, dialog_manager: DialogManager):
        await BotRunner._receive_timetable_search_request(
            message, dialog_manager, message.text
        )
