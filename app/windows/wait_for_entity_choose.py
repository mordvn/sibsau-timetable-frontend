from aiogram_dialog.widgets.text import Const, Format
from aiogram_dialog.widgets.kbd import Button, ListGroup
from aiogram_dialog import Window
from windows.states import BotStates
from aiogram_dialog import StartMode
from aiogram_dialog.widgets.link_preview import LinkPreview

from loguru import logger
from profiler import profile


@profile(func_name="wait_for_entity_choose_getter")
async def getter(dialog_manager, **kwargs):
    entities = dialog_manager.start_data.get("entities", [])
    return {
        "entities": entities,
    }


@profile(func_name="wait_for_entity_choose_on_entity_selected")
async def on_entity_selected(callback, widget, manager):
    item_id = manager.item_id
    entities = manager.start_data.get("entities", [])

    for entity in entities:
        if entity.name == item_id:
            await manager.start(
                BotStates.timetable, mode=StartMode.RESET_STACK, data={"entity": entity}
            )
            return


entity_btn = Button(Format("{item.name}"), id="entity_btn", on_click=on_entity_selected)

wait_for_entity_choose_window = Window(
    Const("Выберите вариант из списка:"),
    ListGroup(
        entity_btn,
        id="entity_list",
        item_id_getter=lambda item: item.name,
        items="entities",
    ),
    LinkPreview(is_disabled=True),
    state=BotStates.wait_for_entity_choose,
    getter=getter,
)
