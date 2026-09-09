import asyncio
import json
import os
import time
import websockets
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("AISSTREAM_API_KEY")
BOUNDING_BOXES = json.loads(os.getenv("AISSTREAM_BOUNDING_BOXES", "[[[-90,-180],[90,180]]]"))
FILTER_MESSAGE_TYPES = json.loads(os.getenv("AISSTREAM_FILTER_MESSAGE_TYPES", '["PositionReport"]'))

URL = "wss://stream.aisstream.io/v0/stream"

async def capture(limit=100):
    captured = []
    start = time.time()
    async with websockets.connect(URL) as ws:
        sub = {
            "APIKey": API_KEY,
            "BoundingBoxes": BOUNDING_BOXES,
            "FilterMessageTypes": FILTER_MESSAGE_TYPES,
        }
        await ws.send(json.dumps(sub))
        print("Subscribed. Capturing...")
        async for message in ws:
            data = json.loads(message)
            data["_received_at"] = time.time()   # ingestion time krusial utk latency
            captured.append(data)
            print(len(captured), "events captured")
            if len(captured) >= limit:
                break
    return captured

async def main():
    limit = int(input("Berapa event mau dikumpulkan? (mis. 500): ") or "500")
    events = await capture(limit)
    with open("profiling/data/raw_sample.jsonl", "w") as f:
        for e in events:
            f.write(json.dumps(e) + "\n")
    print(f"Saved {len(events)} events to profiling/data/raw_sample.jsonl")

asyncio.run(main())