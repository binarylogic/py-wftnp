"""Requires pip install 'wftnp[discovery]'."""

import asyncio

from wftnp.discovery import discover


async def main() -> None:
    for advertisement in await discover():
        print(advertisement.name, advertisement.endpoint)


if __name__ == "__main__":
    asyncio.run(main())
