import asyncio
import pandas as pd
from pymongo import MongoClient
import redis
import json

REDIS_URL = "redis://localhost:6379"
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "medical_data"
SOURCE_COLLECTION = "to_be_scraped_31july_cleaned_links_part1"
SCRAPED_COLLECTION = "to_be_scraped_31july_cleaned_links_output"
QUEUE_BATCH_SIZE = 1000
REFILL_THRESHOLD = 500

async def push_to_queue():
    redis_client = redis.Redis.from_url(REDIS_URL)
    mongo_client = MongoClient(MONGO_URI)
    db = mongo_client[DB_NAME]
    source = db[SOURCE_COLLECTION]
    scraped = db[SCRAPED_COLLECTION]
    scraped_ids = set(scraped.distinct("Record_id"))
    blocked_domains = [
        "quickerala.com","patakare.com","lazoi.com","sehat.com","lybrate.com",
        "ihindustan.com","justdialdds.com","prescripson.com","practo.com",
        "doctoriduniya.com","bajajfinservhealth","doctor360","credihealth.com",
        "deldure.com","curofy.com","rgcross.com","apollo247","ask4healthcare",
        "healthfrog","drdata","drlogy.com","healthworldhospitals.com",
        "medibuddy.in","myupchar.com","docindia.org","askadoctor24x7.com",
        ".pdf","meddco","quickobook","docmitra"
    ]
    regex_pattern = "|".join(blocked_domains)
    cursor = source.find({
        "Record_id": {"$nin": list(scraped_ids)},
        "url": {"$ne": None, "$not": {"$regex": regex_pattern}}
    }, {"_id": 0, "Record_id": 1, "url": 1}).limit(50000)
    batch = []
    total_pushed = 0
    for record in cursor:
        batch.append(json.dumps({
            "Record_id": record["Record_id"],
            "url": record["url"]
        }))
        if len(batch) >= QUEUE_BATCH_SIZE:
            redis_client.lpush("scraping_queue", *batch)
            total_pushed += len(batch)
            batch = []
    if batch:
        redis_client.lpush("scraping_queue", *batch)
        total_pushed += len(batch)
    print(f"Total queued: {total_pushed}")
    mongo_client.close()
    redis_client.close()

async def monitor_queue():
    redis_client = redis.Redis.from_url(REDIS_URL)
    while True:
        queue_size = redis_client.llen("scraping_queue")
        print(f"Queue size: {queue_size}")
        if queue_size < REFILL_THRESHOLD:
            await push_to_queue()
        await asyncio.sleep(30)

if __name__ == "__main__":
    asyncio.run(monitor_queue())
