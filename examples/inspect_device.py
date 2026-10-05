"""Read-only inspection; explicitly opt into UUID reads or notification streams."""

import argparse
import asyncio
from uuid import UUID

from wftnp import Endpoint, WftnpClient


async def inspect(args: argparse.Namespace) -> None:
    async with WftnpClient(Endpoint(args.host, args.port)) as client:
        for service in await client.discover_services():
            print(f"Service {service.uuid}")
            for characteristic in await client.discover_characteristics(service.uuid):
                print(f"  {characteristic.uuid} properties={characteristic.properties!s}")
        for characteristic in args.read:
            value = await client.read_characteristic(characteristic)
            print(f"Read {characteristic}: {value.hex()}")
        if args.subscribe is not None:
            async with await client.subscribe(args.subscribe) as subscription:
                async with asyncio.timeout(args.timeout):
                    for _ in range(args.count):
                        notification = await anext(subscription)
                        print(
                            f"Notification connection={notification.connection_id}: "
                            f"{notification.value.hex()}"
                        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("host")
    parser.add_argument("--port", type=int, default=36866)
    parser.add_argument("--read", type=UUID, action="append", default=[])
    parser.add_argument("--subscribe", type=UUID)
    parser.add_argument("--count", type=int, default=5)
    parser.add_argument("--timeout", type=float, default=15)
    args = parser.parse_args()
    if args.count < 1 or args.timeout <= 0:
        parser.error("count and timeout must be positive")
    asyncio.run(inspect(args))


if __name__ == "__main__":
    main()
