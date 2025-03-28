from aiogram_dialog.widgets.text import Const, Format
from aiogram_dialog.widgets.kbd import Button, Row
from aiogram_dialog.widgets.media import DynamicMedia
from aiogram_dialog import Window
from windows.states import BotStates
from typing import Any
from aiogram.types import ContentType
from aiogram_dialog.api.entities import MediaAttachment
import os
from bs4 import BeautifulSoup
from profiler import profile


@profile(func_name="tutorial_load_tutorials_from_html")
def load_tutorials_from_html():
    tutorials = []
    public_dir = "app/windows/public"

    html_files = [f for f in os.listdir(public_dir) if f.endswith(".html")]
    html_files.sort()

    for html_file in html_files:
        file_path = os.path.join(public_dir, html_file)
        with open(file_path, "r", encoding="utf-8") as f:
            html_content = f.read()

        soup = BeautifulSoup(html_content, "html.parser")

        title = soup.find("h1").text if soup.find("h1") else ""
        description = soup.find("p").text if soup.find("p") else ""

        video_tag = soup.find("video")
        video_path = None
        if video_tag and video_tag.has_attr("src"):
            video_path = os.path.join(public_dir, video_tag["src"])

        tutorials.append(
            {"title": title, "description": description, "video_path": video_path}
        )

    return tutorials


TUTORIAL_SLIDES = load_tutorials_from_html()


@profile(func_name="tutorial_use_effect")
async def getter(dialog_manager, **kwargs):
    if len(TUTORIAL_SLIDES) == 0:
        raise ValueError("No tutorial slides found")

    if "slide_index" not in dialog_manager.dialog_data:
        dialog_manager.dialog_data["slide_index"] = 0
        dialog_manager.dialog_data["total_slides"] = len(TUTORIAL_SLIDES)

    current_slide = dialog_manager.dialog_data.get("slide_index", 0)
    total_slides = dialog_manager.dialog_data.get("total_slides", len(TUTORIAL_SLIDES))

    slide_data = TUTORIAL_SLIDES[current_slide]
    video = None
    if slide_data["video_path"]:
        video = MediaAttachment(ContentType.VIDEO, path=slide_data["video_path"])

    return {
        "current": current_slide + 1,
        "total": total_slides,
        "title": slide_data["title"],
        "description": slide_data["description"],
        "video": video,
    }


@profile(func_name="tutorial_prev_slide")
async def prev_slide(callback, widget, dialog_manager: Any, **kwargs):
    current = dialog_manager.dialog_data.get("slide_index", 0)
    if current > 0:
        dialog_manager.dialog_data["slide_index"] = current - 1


@profile(func_name="tutorial_next_slide")
async def next_slide(callback, widget, dialog_manager: Any, **kwargs):
    current = dialog_manager.dialog_data.get("slide_index", 0)
    total = dialog_manager.dialog_data.get("total_slides", 0)
    if current < total - 1:
        dialog_manager.dialog_data["slide_index"] = current + 1


tutorial_window = Window(
    DynamicMedia("video", when="video"),
    Format("{current}/{total} {title}"),
    Format("{description}"),
    Row(
        Button(
            Const("<<"),
            id="prev_slide",
            on_click=prev_slide,
            when=lambda data, widget, manager: manager.dialog_data.get("slide_index", 0)
            > 0,
        ),
        Button(
            Const(">>"),
            id="next_slide",
            on_click=next_slide,
            when=lambda data, widget, manager: manager.dialog_data.get("slide_index", 0)
            < manager.dialog_data.get("total_slides", 0) - 1,
        ),
    ),
    state=BotStates.tutorial,
    getter=getter,
)
