import asyncio
import logging

from core.aws import AWSManager
from core.config import settings
from storage.storage import Storage

logger = logging.getLogger(__name__)


async def go():
    async with AWSManager() as aws:
        table_name = settings.WALLET_TABLE_NAME

        storage = Storage(aws=aws, table_name=table_name)

        if await storage.table_exists():
            logger.info(f"{table_name=} already exists")
            return

        await storage.create_table(read_capacity=10, write_capacity=10)


def main():
    try:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(go())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
