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
    WeekNumber, 
    DayName, 
    Subgroup,
    LessonType,
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
from datetime import time
from var_dump import var_dump
from aiogram.utils.chat_action import ChatActionSender

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
        if settings.DEBUG:
            logging.basicConfig(level=logging.WARN, stream=sys.stdout)
        await BotRunner.dp.start_polling(BotRunner.bot, skip_updates=True)

    @staticmethod
    @profile(func_name="bot_runner_receive_notification")
    async def receive_notification(message: TimetableChangeData):
        try:
            var_dump(message)  # Оставляем для логов
            if not message.lesson_changes:
                return
                
            users = await database.get_subscribed_users(message.entity.name)
            if not users:
                return
                
            # Создаем простой текст на основе структуры TimetableChangeData
            text = f"🔔 <b>Изменения в расписании: {message.entity.name}</b>\n\n"
            
            # Обрабатываем изменения занятий
            for i, change in enumerate(message.lesson_changes):
                text += f"<b>{change.change_type.value}:</b>\n"
                
                # Для каждого изменения поля
                for field in change.field_changes:
                    field_name = field.field_name
                    
                    # Если это полное занятие (как в примере)
                    if field_name == "lesson":
                        # Добавляем изменение в более читаемом формате
                        lesson_str = str(field.new_value if change.change_type != ChangeType.LESSON_REMOVED else field.old_value)
                        
                        # Извлекаем важную информацию из строки занятия
                        lesson_info = {}
                        if "lesson_name='" in lesson_str:
                            name_part = lesson_str.split("lesson_name='")[1].split("'")[0]
                            lesson_info["name"] = name_part
                            
                        if "time_begin=datetime.time" in lesson_str:
                            time_parts = lesson_str.split("time_begin=datetime.time(")[1].split(")")[0].split(", ")
                            if len(time_parts) >= 2:
                                hour = time_parts[0]
                                minute = time_parts[1]
                                lesson_info["time"] = f"{hour}:{minute}"
                                
                        if "day_name=<DayName." in lesson_str:
                            day_part = lesson_str.split("day_name=<DayName.")[1].split(":")[0]
                            day_mapping = {
                                "MONDAY": "Понедельник",
                                "TUESDAY": "Вторник",
                                "WEDNESDAY": "Среда", 
                                "THURSDAY": "Четверг",
                                "FRIDAY": "Пятница",
                                "SATURDAY": "Суббота",
                                "SUNDAY": "Воскресенье"
                            }
                            lesson_info["day"] = day_mapping.get(day_part, day_part)
                            
                        if "week_number=<WeekNumber." in lesson_str:
                            week_part = lesson_str.split("week_number=<WeekNumber.")[1].split(":")[0]
                            week_mapping = {
                                "ODD": "1 неделя",
                                "EVEN": "2 неделя"
                            }
                            lesson_info["week"] = week_mapping.get(week_part, week_part)
                            
                        if "lesson_type=<LessonType." in lesson_str:
                            type_part = lesson_str.split("lesson_type=<LessonType.")[1].split(":")[0]
                            type_mapping = {
                                "LECTURE": "Лекция",
                                "PRACTICE": "Практика",
                                "LABORATORY": "Лабораторная",
                                "CONSULTATION": "Консультация",
                                "EXAM": "Экзамен"
                            }
                            lesson_info["type"] = type_mapping.get(type_part, type_part)
                            
                        if "auditorium='" in lesson_str:
                            aud_part = lesson_str.split("auditorium='")[1].split("'")[0]
                            if aud_part:
                                lesson_info["aud"] = aud_part
                                
                        if "professors=" in lesson_str:
                            profs_part = lesson_str.split("professors=")[1].split(", location=")[0]
                            profs_part = profs_part.replace("[", "").replace("]", "").replace("'", "")
                            if profs_part and profs_part != "":
                                lesson_info["profs"] = profs_part
                        
                        # Формируем строку для отображения
                        lesson_text = ""
                        if "name" in lesson_info:
                            lesson_text = lesson_info["name"]
                            
                            # Добавляем информацию о дне и времени
                            prefix = ""
                            if "day" in lesson_info and "time" in lesson_info:
                                prefix = f"{lesson_info['day']}, {lesson_info['time']}"
                            elif "time" in lesson_info:
                                prefix = lesson_info["time"]
                                
                            if prefix:
                                lesson_text = f"{prefix} - {lesson_text}"
                                
                            # Добавляем информацию о типе, аудитории и т.д.
                            details = []
                            if "type" in lesson_info:
                                details.append(lesson_info["type"])
                            if "aud" in lesson_info:
                                details.append(lesson_info["aud"])
                            if "week" in lesson_info:
                                details.append(lesson_info["week"])
                                
                            if details:
                                lesson_text += f" ({', '.join(details)})"
                                
                            # Добавляем информацию о преподавателях
                            if "profs" in lesson_info:
                                lesson_text += f"\nПреподаватели: {lesson_info['profs']}"
                                
                        text += f"• {lesson_text}\n\n"
                    else:
                        # Для других типов полей
                        old_val = field.old_value
                        new_val = field.new_value
                        
                        # Форматируем значения для отображения
                        if change.change_type == ChangeType.LESSON_MODIFIED:
                            text += f"• {field_name}: {old_val} → {new_val}\n"
                        elif change.change_type == ChangeType.LESSON_ADDED:
                            text += f"• {field_name}: {new_val}\n"
                        elif change.change_type == ChangeType.LESSON_REMOVED:
                            text += f"• {field_name}: {old_val}\n"
                
                text += "\n"
            
            if len(text) > 4000:
                text = text[:3950] + "...\n\n<i>Сообщение слишком длинное. Показаны не все изменения.</i>"
                
            for user in users:
                try:
                    await BotRunner.bot.send_message(
                        user, text, parse_mode=ParseMode.HTML
                    )
                except Exception as e:
                    logger.warning(f"Error sending notification to user {user}: {e}")

        except Exception as e:
            logger.warning(f"Error processing notification: {e}")

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
