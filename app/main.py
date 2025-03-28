import asyncio
from loguru import logger

from logger import configure_logging
from config import settings

from database import database
from broker import Broker
from bot import BotRunner


async def background_main(broker: Broker):
    logger.info("Starting message consumer")
    while True:
        if BotRunner.bot is None or BotRunner.dp is None:
            BotRunner.init(settings.BOT_TOKEN)
            await asyncio.sleep(1)
            continue

        changes = await broker.get_message()
        if changes:
            await BotRunner.receive_notification(changes)
        await asyncio.sleep(1)


async def main():
    logger.info("Starting up")
    await BotRunner.init(settings.BOT_TOKEN)

    await database.initialize()
    async with Broker(connection_string=settings.RABBITMQ_URI) as broker:
        asyncio.create_task(background_main(broker))

        logger.info("Starting bot")
        await BotRunner.run_bot()
    await database.close()


if __name__ == "__main__":
    configure_logging()

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt")
    except Exception as e:
        logger.exception(f"Error when starting: {e}")
