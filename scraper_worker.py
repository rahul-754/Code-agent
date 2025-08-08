import asyncio
import aiohttp
import json
import logging
import random
import socket
from pymongo import MongoClient
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
import redis
from bs4 import BeautifulSoup
import requests

MASTER_IP = "192.168.1.42"  # Change to your master machine's LAN IP
PROXY = None  # e.g. "http://user:pass@host:port" or None
REDIS_URL = f"redis://{MASTER_IP}:6379"
MONGO_URI = f"mongodb://{MASTER_IP}:27017/"
DB_NAME = "medical_data"
OUTPUT_COLLECTION = "to_be_scraped_31july_cleaned_links_output"
CHROMIUM_EXECUTABLE_PATH = r"C:\Users\Desk0012\AppData\Local\ms-playwright\chromium-1179\chrome-win\chrome.exe"

CONCURRENCY = 50
HTTP_TIMEOUT = 15
BROWSER_TIMEOUT = 45000  # ms
BATCH_INSERT_SIZE = 1  # Increased batch size for efficiency
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_0) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
]
WORKER_ID = socket.gethostname()
logging.basicConfig(level=logging.INFO, format=f"%(asctime)s [{WORKER_ID}] %(message)s")
logger = logging.getLogger(__name__)


async def fetch_static(session, url):
    """
    Fetch the page using HTTP and extract visible text using BeautifulSoup.
    Returns (visible_text, "static") or (None, None) on failure.
    """
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive"
    }
    try:
        async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=HTTP_TIMEOUT)) as response:
            if response.status == 200:
                html = await response.text()
                # Quick sanity check for doctor's page
                if len(html) > 5000 and any(keyword in html.lower() for keyword in ["doctor", "dr.", "clinic"]):
                    soup = BeautifulSoup(html, "html.parser")
                    visible_text = soup.get_text(separator="\n", strip=True)
                    return visible_text, "static"
    except Exception as e:
        logger.debug(f"Static HTTP fetch failed for {url}: {e}")
    return None, None


async def simulate_human_interaction(page):
    """
    Simulate human mouse movements and scrolling to avoid bot detection.
    """
    for _ in range(random.randint(3, 6)):
        x = random.randint(100, 800)
        y = random.randint(100, 600)
        await page.mouse.move(x, y)
        await asyncio.sleep(random.uniform(0.1, 0.3))
    for _ in range(3):
        await page.mouse.wheel(0, random.randint(300, 600))
        await asyncio.sleep(random.uniform(0.3, 0.7))
    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    await asyncio.sleep(random.uniform(1.0, 2.0))


async def click_expandable_elements(page):
    """
    Click on expandable elements/buttons that can reveal more info on the page.
    """
    expandable_keywords = [
        "read more", "show more", "see more", "view more", "learn more",  # kept generic expanders
        "membership", "member", "memberships",
        "award", "awards",
        "education", "qualifications", "certifications",
        "certified", "license", "licenses", "license number", "registration number", "registration number",
        "clinic", "clinic info", "clinic details", "practice location", "practice locations", "practice address", "practice addresses",
        "experience", "professional experience", "clinical experience",
        "publications", "research", "publications and research",
        "languages spoken", "languages",
        "contact details", "contact info"
    ]

    # Only query buttons and divs with role='button' to avoid navigation links (<a>)
    buttons = await page.query_selector_all("button, div[role='button']")
    for btn in buttons:
        try:
            text = (await btn.inner_text() or "").strip().lower()
            if not any(key in text for key in expandable_keywords):
                continue
            logger.info(f"Clicking on expandable element: '{text}'")
            await btn.click(timeout=3000)
            await asyncio.sleep(1)
        except Exception:
            continue



async def close_common_popups_and_verify(page):
    """
    Close popups, cookie notices, captcha modals or verification buttons to access the page content.
    """
    keywords = [
        "accept", "accept all", "accept cookies", "agree", "agree all", "got it",
        "allow", "allow all", "accept and continue", "accept terms", "i agree", "i accept", "understood", "continue with cookies",
        "verify", "verify now", "verify you", "i am human", "i'm human", "i’m not a robot", "i am not a robot",
        "not a robot", "check", "confirm identity", "i'm not a bot", "click to verify", "solve captcha", "complete verification",
        "recaptcha", "close", "dismiss", "cancel", "skip", "no thanks", "not now", "maybe later",
        "exit", "x", "✕", "✖", "✘", "🗙", "×", "leave", "hide", "remove",
        "ok", "okay", "yes", "continue", "confirm", "proceed", "next", "start now",
        "let's go", "view site", "enter site", "go to site", "continue to site", "resume", "view anyway", "access site",
        "start browsing", "launch", "go ahead", "gotcha", "take me there", "let me in", "start",
        "not interested", "maybe later", "no, thanks", "remind me later", "already have an account",
        "sign in later", "login later", "continue as guest", "skip login", "skip signup"
    ]
    buttons = page.locator("button, input[type='button'], div[role='button']")
    count = await buttons.count()
    for i in range(count):
        try:
            el = buttons.nth(i)
            text = (await el.inner_text()).lower()
            if any(k in text for k in keywords):
                await el.click(timeout=2000)
                await asyncio.sleep(0.5)
        except:
            continue


async def fetch_with_browser(browser, url):
    """
    Fetch page content with Playwright browser.
    Creates a new browser context per URL with resource blocking for speed.
    Returns visible text and method 'browser'.
    """
    try:
        context = await browser.new_context(
            user_agent=random.choice(USER_AGENTS),
            viewport={"width": 1920, "height": 1080},
            java_script_enabled=True,
            locale="en-US",
        )

        # Block images, css, fonts, media to speed up loading
        async def handle_route(route, request):
            if request.resource_type in ["image", "stylesheet", "font", "media"]:
                await route.abort()
            else:
                await route.continue_()

        await context.route("**/*", handle_route)

        page = await context.new_page()
        await page.goto(url, timeout=BROWSER_TIMEOUT, wait_until="domcontentloaded")
        await simulate_human_interaction(page)
        await close_common_popups_and_verify(page)
        await click_expandable_elements(page)
        await simulate_human_interaction(page)
        text_content = await page.inner_text("body")
        await context.close()
        return text_content, "browser"
    except PlaywrightTimeoutError:
        logger.error(f"[Timeout] Browser fetch timed out {url}")
    except Exception as e:
        logger.error(f"Browser fetch failed for {url}: {e}")
    return None, None


def _parse_json_lax(raw: str):
    try:
        return json.loads(raw)
    except Exception:
        try:
            start = raw.find("{")
            end = raw.rfind("}") + 1
            if start != -1 and end != -1:
                return json.loads(raw[start:end])
        except Exception:
            return None
    return None


def extract_with_llm(text_content, record_id, url):
    """
    Performs LLM extraction of doctor info from visible text content.
    Sends only cleaned text to LLM; no raw HTML.
    """
    prompt = f"""
Extract the following doctor information as JSON from the text below. Only include what is explicitly visible in the content. If anything is missing or unclear, set it as "NA". No guessing or hallucination.
TEXT:
\"\"\"
{text_content}
\"\"\"
FIELDS:
{{
"Prefix": "",
"First Name": "",
"Last Name": "",
"Name": "",
"HCP Type": "",
"Age": "",
"Experience": "",
"HCP_speciality1": "",
"Degree_1": "",
"Degree_2": "",
"Degree_3": "",
"Degree_4": "",
"Degree_5": "",
"Graduation Year 1": "",
"UG college name": "",
"PG college name": "",
"Memberships": [],
"Awards": [],
"HCP_Email": "",
"Verification URL": "",
"HCO ID": "",
"HCO_Name": "",
"HCO_Speciality": "",
"Clinics": [
    {{"HCO_Name": "", "HCO_Speciality": "", "Address": "", "City": "", "State": "", "Pincode": "", "Plus Code": "", "Latitude": "", "Longitude": "", "HCO Type": "", "HCO Website": "", "HCO Phone": ""}}
],
"Address": "",
"City": "",
"State": "",
"Pincode": "",
"Plus Code": "",
"Latitude": "",
"Longitude": "",
"License_Number1": "",
"Licensing_Body1": "",
"Licensing_Year1": "",
"Focus Area": "",
"Phone": "",
"HCO Type": "",
"HCO Website": "",
"HCO Phone": ""
}}
RULES:
- strictly you should give only one doctor's information per JSON object.
- If a field is not present, set it to "NA".
- Output must match this structure.
- Clinics upto 5 entries, each with its own address and details.
- Max 5 items for lists (degrees, awards,memberships etc).
- Integers only for numbers (experience, fees).
- Match label variants (e.g., "Practice Location" → "Address").
- Scan all visible sections: headers, tabs, cards, tables, footers, etc.
""".strip()

    url = "https://www.blackbox.ai/api/chat"
    headers = {
        "accept": "*/*",
        "accept-encoding": "gzip, deflate, br, zstd",
        "accept-language": "en-US,en;q=0.9",
        "content-type": "application/json",
        "origin": "https://www.blackbox.ai",
        "referer": "https://www.blackbox.ai/",
        "user-agent": USER_AGENTS[0],
        # Fill in your cookie here for Blackbox (live session required)
        "cookie": "...(your blackbox cookies)...",
    }
    payload = {
        "messages": [
            {
                "id": "3BcP8g5",
                "content": prompt,
                "role": "user"
            }
        ],
        "id": "3BcP8g5",
        "previewToken": None,
        "userId": None,
        "codeModelMode": True,
        "trendingAgentMode": {},
        "isMicMode": False,
        "userSystemPrompt": None,
        "maxTokens": 1024,
        "playgroundTopP": None,
        "playgroundTemperature": None,
        "isChromeExt": False,
        "githubToken": "",
        "clickedAnswer2": False,
        "clickedAnswer3": False,
        "clickedForceWebSearch": False,
        "visitFromDelta": False,
        "isMemoryEnabled": False,
        "mobileClient": False,
        "userSelectedModel": None,
        "userSelectedAgent": "VscodeAgent",
        "validated": "a38f5889-8fef-46d4-8ede-bf4668b6a9bb",
        "imageGenerationMode": False,
        "imageGenMode": "autoMode",
        "webSearchModePrompt": False,
        "deepSearchMode": False,
        "domains": None,
        "vscodeClient": False,
        "codeInterpreterMode": False,
        "customProfile": {
            "name": "",
            "occupation": "",
            "traits": [],
            "additionalInfo": "",
            "enableNewChats": False
        },
        "webSearchModeOption": {
            "autoMode": True,
            "webMode": False,
            "offlineMode": False
        }
    }

    try:
        res = requests.post(url, headers=headers, json=payload, timeout=200)
        if res.status_code == 200:
            raw = res.text.strip()
            data = _parse_json_lax(raw)
            return data, raw, res.status_code
    except Exception as e:
        logger.error(f"LLM extraction failed: {e}")
    return None, "", 500


async def process_single_url(session, browser, record):
    """
    Process a single URL:
    - Check DB if already processed
    - Fetch visible text (prefer browser fetch, fallback to static fetch)
    - Extract info with LLM
    - Return structured result for DB insert
    """
    record_id = record.get("Record_id")
    url = record.get("url")

    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    output_collection = db[OUTPUT_COLLECTION]
    # Skip if already processed
    if output_collection.find_one({"Record_id": record_id}):
        client.close()
        return None
    client.close()

    # Fetch content with browser context reuse
    text_content, method = await fetch_with_browser(browser, url)
    if not text_content or len(text_content.strip()) < 100:
        # Fallback to static fetch
        text_content, method = await fetch_static(session, url)
    if not text_content or len(text_content.strip()) < 100:
        logger.warning(f"Failed to fetch usable content for URL {url}")
        return None

    structured_data, raw_response, status_code = extract_with_llm(text_content, record_id, url)
    return {
        "source_url": url,
        "structured_data": structured_data,
        "raw_response": raw_response,
        "Record_id": record_id,
        "text_content": text_content,
        "status_code": status_code,
        "extraction_method": method,
        "worker_id": WORKER_ID
    }


async def batch_insert_to_mongo(documents):
    if not documents:
        return
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    output_collection = db[OUTPUT_COLLECTION]
    try:
        output_collection.insert_many(documents, ordered=False)
        logger.info(f"Inserted {len(documents)} documents to MongoDB collection '{OUTPUT_COLLECTION}'")
    except Exception as e:
        logger.error(f"Batch insert failed: {e}")
    finally:
        client.close()


async def worker():
    logger.info(f"Worker starting; REDIS={REDIS_URL} / MONGO={MONGO_URI}")
    redis_client = redis.Redis.from_url(REDIS_URL)
    playwright = await async_playwright().start()
    browser = await playwright.chromium.launch(
        headless=True,
        executable_path=CHROMIUM_EXECUTABLE_PATH,
        args=["--disable-blink-features=AutomationControlled", "--no-sandbox", "--window-size=1920,1080"]
    )

    timeout = aiohttp.ClientTimeout(total=HTTP_TIMEOUT)
    connector = aiohttp.TCPConnector(limit=100, limit_per_host=20)
    async with aiohttp.ClientSession(timeout=timeout, connector=connector,
                                     headers={
                                         "User-Agent": random.choice(USER_AGENTS),
                                         "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                                         "Accept-Language": "en-US,en;q=0.9",
                                         "Connection": "keep-alive",
                                     }) as session:

        semaphore = asyncio.Semaphore(CONCURRENCY)
        pending_documents = []
        processed_count = 0  # Counter for processed URLs

        async def process_with_semaphore(record):
            nonlocal processed_count
            async with semaphore:
                processed_count += 1
                logger.info(f"Processing URL #{processed_count}: {record.get('url')}")
                return await process_single_url(session, browser, record)

        while True:
            tasks = []
            for _ in range(min(CONCURRENCY, 10)):
                data = redis_client.brpop("scraping_queue", timeout=5)
                if data:
                    record = json.loads(data[1])
                    task = asyncio.create_task(process_with_semaphore(record))
                    tasks.append(task)
            if not tasks:
                logger.info("No queue tasks, sleeping 10s...")
                await asyncio.sleep(10)
                continue

            results = await asyncio.gather(*tasks, return_exceptions=True)

            for r in results:
                if r and not isinstance(r, Exception):
                    pending_documents.append(r)

            if len(pending_documents) >= BATCH_INSERT_SIZE:
                await batch_insert_to_mongo(pending_documents)
                pending_documents = []

    await browser.close()
    await playwright.stop()




if __name__ == "__main__":
    asyncio.run(worker())
