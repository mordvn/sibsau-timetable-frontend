from aiogram.filters.state import State, StatesGroup


class BotStates(StatesGroup):
    wait_for_entity_choose = State()
    timetable = State()
    tutorial = State()
