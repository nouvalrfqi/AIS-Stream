import asyncio
import json
import random
import time

import websockets


def _delay_seconds(config: dict, retry: int) -> float:
    base = config["reconnect_base_seconds"]
    cap = config["reconnect_max_seconds"]
    jitter = config["reconnect_jitter_seconds"]
    backoff = min(base * (2 ** retry), cap)
    return backoff + random.uniform(0, jitter)


def _build_subscription(config: dict) -> dict:
    return {
        "APIKey": config["api_key"],
        "BoundingBoxes": config["bounding_boxes"],
        "FilterMessageTypes": config["filter_message_types"],
    }


async def consume(config: dict, on_event, log) -> None:
    retry = 0
    subscription = _build_subscription(config)

    while True:
        try:
            async with websockets.connect(config["aisstream_url"]) as ws:
                await ws.send(json.dumps(subscription))
                if retry > 0:
                    log("info", "websocket reconnected", retry=retry)
                else:
                    log("info", "websocket connected")
                retry = 0

                async for message in ws:
                    try:
                        raw = json.loads(message)
                    except json.JSONDecodeError:
                        log("warn", "non-JSON websocket frame ignored")
                        continue
                    raw["_received_at"] = time.time()
                    await on_event(raw)

        except websockets.exceptions.ConnectionClosed as exc:
            log("warn", "websocket connection closed", code=exc.code, reason=exc.reason)
        except Exception as exc:
            log("error", "websocket error", error=str(exc), type=type(exc).__name__)

        delay = _delay_seconds(config, retry)
        log("info", "reconnecting", retry=retry, delay_seconds=round(delay, 2))
        await asyncio.sleep(delay)
        retry += 1