from __future__ import annotations

import asyncio
import dataclasses
from datetime import datetime, timedelta
import os
import re
import sys
import time
from dataclasses import dataclass
from typing import List, Union

import httpx
import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
from loguru import logger
from playwright.async_api import (
    Locator,
    Page,
    Request,
    Route,
    async_playwright,
    Browser,
)
from playwright.sync_api import sync_playwright
from tqdm import tqdm
import playwright
from typing import Any
from pprint import pprint
import ray
from uuid import uuid4
import glob
import duckdb
import ujson as json

ray.init(
    local_mode=False,
    log_to_driver=True,
    dashboard_host="localhost",
    dashboard_port=8265,
)
"""
Change the logger level to debug for verbose
"""
logger.remove()
logger.add(sys.stderr, level="DEBUG")


@dataclass
class Content_Data:
    question_id: int
    answer_id: int
    author_id: str
    question_title: str
    content: str
    upvotes: str
    answer_creation_time: str

@dataclass
class Scrape_Question_Config:
    batch_size: int
    worker_num: int
    num_scroll: int
    headless: bool


@dataclass
class Download_Answer_Config:
    parallel_req: int
    save_interval : int


@dataclass
class Scrape_Common_Topic_Config:
    batch_size: int
    scroll_down_num : int
    headless : bool
    

async def get_all_href(page: Union[Page, Locator]) -> List[str]:
    hrefs = await page.evaluate(
        """() => {
            let links = document.querySelectorAll('[href]');
            let hrefs = [];
            for (let link of links) {
                hrefs.push(link.href);
            }
            return hrefs;
        }"""
    )
    valid_hrefs = [x for x in hrefs if isinstance(x, str) and "https://" in x]
    return valid_hrefs




async def get_questions(page: Page, topic_url: str):
    await page.goto(topic_url)
    all_hrefs = await get_all_href(page)
    question_urls = set(
        [x for x in all_hrefs if "/question/" in x and "waiting" not in x]
    )
    return question_urls


async def intercept_request(route: Route, request: Request, req_to_abort: List[str]):
    regex = re.compile("|".join(req_to_abort), flags=re.IGNORECASE)

    if regex.search(pos=10, string=request.url):
        # logger.debug(f"Abort request :{request.url}")
        await route.abort()
    else:
        headers = request.headers.copy()

        await route.continue_(headers=headers)


async def get_questions(page: Page, topic_url: str):
    await page.goto(topic_url)
    all_hrefs = await get_all_href(page)
    question_urls = set(
        [x for x in all_hrefs if "/question/" in x and "waiting" not in x]
    )
    return question_urls


async def intercept_request(route: Route, request: Request, req_to_abort: List[str]):
    regex = re.compile("|".join(req_to_abort), flags=re.IGNORECASE)
    if regex.search(pos=10, string=request.url):
        logger.debug(f"Abort request :{request.url}")
        await route.abort()
    else:

        await route.continue_()

"""
We do not need to scrape round table topic repeatedly because round table topics are only 1.6k in total and it is growing very slow
"""


def scrape_round_tables(headless=True):
    output_dir = "./data"

    roundtable_topic_scrolldown = 200
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless, timeout=60000)
        page = browser.new_page()
        page.goto("https://zhihu.com/roundtable")
        # Scroll down roundtable topic to get more topic urls
        for _ in range(roundtable_topic_scrolldown):
            page.keyboard.down("End")
            page.wait_for_timeout(500)
        hrefs = get_all_href(page)
        relevent_hrefs = [x for x in hrefs if "https://www.zhihu.com/roundtable/" in x]
        round_table_df = pd.DataFrame({"round_table_topic_url": relevent_hrefs})
        round_table_df.to_csv(f"{output_dir}/round_table_topics.csv")





async def cancel_pop_up(page: Page):
    await page.wait_for_timeout(1000)
    await page.locator(".Modal-closeButton").click()


async def get_question_answer_urls_from_page(page: Page, topic_url: str, scrolldown=5):
    # await page.wait_for_timeout(np.random.randint(1000, 3500))

    await page.goto(topic_url)
    # await page.wait_for_timeout(np.random.randint(1000, 3500))

    await cancel_pop_up(page)
    # await page.wait_for_timeout(np.random.randint(1000, 3500))
    for _ in tqdm(range(scrolldown), desc="Scroll down"):
        await page.keyboard.down("End")
        await page.wait_for_timeout(500)

    all_hrefs = await get_all_href(page)
    question_urls = set(
        [
            x for x in all_hrefs if "/question/" in x and "/answer/" in x
        ]  # this will remove examples like https://zhuanlan.zhihu.com/p/336435588
    )
    return question_urls


async def weird_ritual(browser: Browser, device: Any):
    try:
        context = await browser.new_context(
            **device, java_script_enabled=False, screen={"width": 1920, "height": 1200}
        )
        page: Page = await context.new_page()

        await page.goto("https://www.zhihu.com/topic", timeout=3000)
        await context.close()

    except Exception as e1:
        logger.info(e1)


"""
We do not need to scrpae round table topic repeatedly because round table topics are only 1.6k in total and it is growing very slow
"""


def scrape_round_tables(headless=True):
    output_dir = "./data"

    roundtable_topic_scrolldown = 200
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless, timeout=60000)
        page = browser.new_page()
        page.goto("https://zhihu.com/roundtable")
        # Scroll down roundtable topic to get more topic urls
        for _ in range(roundtable_topic_scrolldown):
            page.keyboard.down("End")
            page.wait_for_timeout(500)
        hrefs = get_all_href(page)
        relevent_hrefs = [x for x in hrefs if "https://www.zhihu.com/roundtable/" in x]
        round_table_df = pd.DataFrame({"round_table_topic_url": relevent_hrefs})
        round_table_df.to_csv(f"{output_dir}/round_table_topics.csv")


def get_base_topic():
    return [
        "生活方式",
        "经济学",
        "运动",
        "互联网",
        "艺术",
        "阅读",
        "美食",
        "动漫",
        "汽车",
        "教育",
        "摄影",
        "历史",
        "文化",
        "旅行",
        "职业发展",
        "金融",
        "游戏",
        "篮球",
        "生物学",
        "物理学",
        "化学",
        "科技",
        "体育",
        "商业",
        "健康",
        "创业",
        "设计",
        "自然科学",
        "法律",
        "电影",
        "音乐",
        "投资",
    ]


'''
Process 1 : Get all common topic links from base topic
i.e. 文化(base topic) -> 节日(common topic).
Base topic is the highest level of abstraction for zhihu question category that we can find at the moment.
This steps does not requires parallelism as this steps only run once.
Expect this step to take a maximum of a few hours to finish.
'''
async def scrape_common_topics_async(config : Scrape_Common_Topic_Config):
    output_dir = "./data"
    os.makedirs(output_dir, exist_ok=True)
    logger.success(f"Output Dir : {output_dir}")
    """
    Parameters
    ----------
    batch_size : how many page to open simultaneously
    scroll_down_num : how many times to scroll down for capturing common topic from a pge in base topic
    headless :  True if running in headless mode (works in server), 
                False for spawning the real browser (Might not work in server setup) 

    Return
    ------
    None
    
    Output 
    ----
    Output files to ./data/common_topics.csv
    """
    base_topic = get_base_topic()

    batch_topics = []
    for i in range(0, len(base_topic), config.batch_size):
        batch_topics.append(base_topic[i : i + config.batch_size])
    output_files = f"{output_dir}/common_topics.csv"
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=config.headless, timeout=60000)
        for batch in batch_topics:
            page_traverse_cor = []
            current_pages: List[(str, Page)] = []
            for topic in batch:
                '''
                Open multiple page in same browser instance
                '''
                page = await browser.new_page()
                page_traverse_cor.append(page.goto(f"https://zhihu.com/topics#{topic}"))
                current_pages.append((topic, page))
            await asyncio.gather(*page_traverse_cor)
            all_df = pd.DataFrame()
            skip_page_id = []
            '''
                Scroll down to capture more common topic 
                i.e. 文化(base topic) -> 节日(common topic)
            '''
            for _ in tqdm(range(config.scroll_down_num)):
                for page_id, (topic_cat, topic_page) in enumerate(current_pages):
                    try:
                        if page_id in skip_page_id:
                            continue
                        if len(skip_page_id) == len(current_pages):
                            break
                        await topic_page.locator(
                            "body > div.zg-wrap.zu-main.clearfix > div.zu-main-content > div > div > div.zm-topic-cat-sub > a:nth-child(2)"
                        ).click(timeout=3000)
                        await topic_page.keyboard.down("End")
                        await topic_page.wait_for_timeout(1000)
                    except Exception as e2:
                        logger.warning(e2)
                        skip_page_id.append(page_id)
                        if len(skip_page_id) == len(current_pages):
                            break
                    hrefs = await get_all_href(topic_page)
                    relevent_hrefs = [
                        x for x in hrefs if "https://www.zhihu.com/topic/" in x
                    ]
                    topic_df = pd.DataFrame({"topic_urls": relevent_hrefs})
                    topic_df["basic_topic_category"] = topic_cat
                    all_df = pd.concat([all_df, topic_df])
                '''
                Persist the data to csv file repeatedly 
                '''
                if os.path.exists(output_files):
                    all_df.to_csv(f"{output_dir}/common_topics.csv", header=False, mode="a")
                else:
                    all_df.to_csv(f"{output_dir}/common_topics.csv")
            # Closes all pages
            [await x.close() for _, x in current_pages]


@ray.remote
class Scrape_Worker(object):
    def __init__(self):
        pass
   
    async def scrape_question_answer_url(
        self,
        common_topic_hrefs: List[str],
        headless: bool,
        scroll_down_num: int,
        all_viewed_questions: List[int],
    ):
        """
        Parameters
        ----------
        common_topic_hrefs : list of common topics
        headless : True if running in headless mode (works in server), 
                    False for spawning the real browser (Might not work in server setup)  
        scroll_down_num: How many times to scroll down to capture questions in 1 common topic
        all_viewed_questions : downloaded questions id (to avoid loading repeated questions)
        
        Return
        ------
        None
        
        Output
        ----
        Output files to ./data/scrape_question_answer/*.csv
        
        """
        logger.debug("Starts scraping")
        logger.debug(f"Common topic urls to scrape : {len(common_topic_hrefs)}")
        pattern = r"/question/\d+/answer/\d+"
        outputDir = "data/scrape_question_answer"
        os.makedirs(outputDir, exist_ok=True)
        cache_seen_link = set()
        cache_seen_question = set()
        all_payloads = []
        try:
            async with async_playwright() as p:
                for topic_url in tqdm(common_topic_hrefs, desc="Common topics"):
                    try:
                        logger.success(f"Starts {topic_url} scroll down")
                        browser = await p.firefox.launch(
                            headless=headless, timeout=100000000
                        )

                        # await weird_ritual(browser, device)
                        context = await browser.new_context()
                        context.set_default_timeout(15000)
                        page: Page = await context.new_page()
                        question_urls = await get_question_answer_urls_from_page(
                            page, topic_url, scrolldown=scroll_down_num
                        )
                        logger.warning(
                            f"Captured {len(set(question_urls))} unique questions from this topic"
                        )
                        question_urls_in_order = sorted(list(question_urls))
                        for qUrl in question_urls_in_order:
                            qUrl = qUrl.replace("?write", "")
                            qId = qUrl.split("/")[-3]
                            # Skip question page if question has already been visited before in previous runs
                            if int(qId) in all_viewed_questions:
                                continue
                            # Skip question page if question has already been visited in current runs

                            if qId in cache_seen_question:
                                continue
                            else:
                                cache_seen_question.add(qId)
                            await page.goto(qUrl, wait_until="commit")
                            await page.wait_for_timeout(1000)
                            await cancel_pop_up(page)

                            question_title_cor = await page.locator(
                                ".QuestionHeader-title"
                            ).all_inner_texts()
                            question_title = question_title_cor[0]
                            """
                            Scroll down in questions and load more answers from same questions
                            """
                            try:
                                await page.locator("div.ViewAll:nth-child(5)").click(
                                    timeout=2000
                                )
                                for s_id in range(5):
                                    await page.wait_for_timeout(1000)
                                    await page.keyboard.down("End")
                                    logger.debug(f"scrolling down answers : {s_id + 1}")

                            except Exception as e3:
                                logger.debug(e3)
                            href_comp = page.locator(".Question-main")

                            all_hrefs = await get_all_href(href_comp)
                            matches_question_answer_url = set(
                                [
                                    s
                                    for s in all_hrefs
                                    if isinstance(s, str)
                                    and re.search(pattern, s)
                                    and qId in s
                                ]
                            )

                            for k in matches_question_answer_url:
                                try:
                                    if k in cache_seen_link:
                                        continue
                                    else:
                                        cache_seen_link.add(k)
                                    elem = k.split("/")
                                    qId = int(elem[-3].replace("#!", ""))
                                    aId = int(elem[-1].replace("#!", ""))
                                    all_payloads.append(
                                        {
                                            "qId": qId,
                                            "aid": aId,
                                            "question_title": question_title,
                                            "topic_url": topic_url,
                                        }
                                    )
                                except Exception as e2:
                                    logger.error(e2, k)
                            logger.success(
                                f"Received {len(all_payloads)} answers urls from this topic. Latest question : {question_title}"
                            )
                    except Exception as e1:
                        logger.error(e1)
                    finally:
                        await browser.close()
        except Exception as e3:
            logger.error(e3)
        # Persist data periodically
        if len(all_payloads) > 0:
            tmp_df = pd.json_normalize(all_payloads)
            filename = f"{uuid4().hex}.csv"
            tmp_df.to_csv(f"{outputDir}/{filename}")
            logger.success(f"Saved to {outputDir}/{filename}.csv")


def is_20_minute_interval(start_time, current_time):
    start_time = datetime.fromtimestamp(start_time)
    current_time = datetime.fromtimestamp(current_time)
    restart_interval_minute = start_time + timedelta(minutes=20)
    return current_time > restart_interval_minute





async def scrape_question_urls(config:Scrape_Question_Config):
    all_round_table_df = pd.read_csv("data/common_topics.csv")
    all_common_topics = all_round_table_df["topic_urls"].tolist()

    current_downloaded = pd.read_parquet("downloaded_question_answer.parquet")
    all_viewed_questions = current_downloaded["question_id"].astype(int).tolist()

    common_topic_hrefs = all_common_topics
    np.random.shuffle(common_topic_hrefs)
    logger.info("Starts scraping")
    logger.info(f"Detected downloaded topic : {len(all_common_topics)}")
    logger.info(f"Common topic urls to scrape : {len(common_topic_hrefs)}")
    batch_task = []

    for i in range(0, len(common_topic_hrefs), config.batch_size):
        batch_task.append(common_topic_hrefs[i : i + config.batch_size])

    running_actor = []
    all_actors = []
    start_time = time.time()
    for id, tasks in tqdm(enumerate(batch_task), desc="batch_task"):
        actor: Scrape_Worker = Scrape_Worker.remote()
        all_actors.append(actor)
        actor_ref = actor.scrape_question_answer_url.remote(
            tasks, config.headless, config.num_scroll, all_viewed_questions
        )
        running_actor.append(actor_ref)

        if len(running_actor) >= config.worker_num:
            logger.success("Waiting for next actor to finish running")
            # Block system until next worker has completed the task to maintain number of active worker
            ready, not_ready = ray.wait(running_actor)
            running_actor = not_ready
            
        # Killing all actors to prevent dead headless browser instance hogs all memory
        if is_20_minute_interval(start_time, time.time()):
            logger.warning(
                f"Killing all actors to prevent dead headless browser instance hogs all memory. \
                Current working actors num : {len(running_actor)}"
            )
            [ray.kill(x) for x in all_actors]
            all_actors.clear()
            running_actor.clear()
            start_time = time.time()



async def get_answer_content(
    qid: int, aid: int, question_str: str, req_client: httpx.AsyncClient, proxy: str
) -> str:
    """
    根据回答ID和问题ID获取回答内容
    Parameters
    ----------
    qid : 问题ID
    aid : 回答ID
    例如一个回答链接为: https://www.zhihu.com/question/438404653/answer/1794419766
    其 qid 为 438404653
    其 aid 为 1794419766
    注意,这两个参数均为字符串
    Return
    ------
    str : 回答内容
    """
    url = f"https://www.zhihu.com/question/{qid}/answer/{aid}"

    response = await req_client.get(url, timeout=60000)
    assert response.status_code == 200

    soup = BeautifulSoup(response.text, "html.parser")
    content = " ".join([p.text.strip() for p in soup.find_all("p")])
    # raise exception if content is not valid
    if content == "你似乎来到了没有知识存在的荒原" or "var BC_IP_PORT = 9000;" in content:
        raise Exception("No answers for this question")
    """
        "<meta itemProp="dateCreated" content="2023-02-20T13:19:30.000Z"/>"
        last time from meta tag with item prop attributes seems to be the post creation datetime. I verified by looking at page online

    """
    answer_creation_time_div = soup.find_all(
        "meta",
        {"itemprop": "dateCreated"},
    )
    answer_creation_time_content = ""
    if len(answer_creation_time_div) > 0:
        answer_creation_time_content = answer_creation_time_div[-1].attrs["content"]
    upvotes = (
        soup.find(
            "button",
            {"class": "Button VoteButton VoteButton--up"},
        )
        .get_text()
        .replace("\u200b", "")
    )
    author_ids = soup.find_all(
        "meta",
        {"itemprop": "url"},
    )
    author_id_div = [x for x in author_ids if "/people/" in x.attrs["content"]]
    author_id = author_id_div[0].attrs["content"]
    await req_client.aclose()
    return Content_Data(
        question_id=qid,
        answer_id=aid,
        author_id=author_id,
        question_title=question_str,
        content=content,
        upvotes=upvotes,
        answer_creation_time=answer_creation_time_content,
        proxy=proxy,
        request_url=url,
    )


def seed_db(uploaded_df, conn):
    print(
        duckdb.sql(
            """CREATE TABLE IF NOT EXISTS raw_zhihu_answers 
            AS SELECT * FROM uploaded_df""",
            connection=conn,
        )
    )
    print(duckdb.commit(conn))
    print(
        duckdb.sql(
            """INSERT INTO raw_zhihu_answers SELECT * FROM uploaded_df""",
            connection=conn,
        )
    )
    print(duckdb.commit(conn))
    print(duckdb.sql("SHOW TABLES", connection=conn))


def init_db():
    conn = duckdb.connect(
        database="./db/zhihu.db",
        read_only=False,
    )
    duckdb.sql("SET GLOBAL pandas_analyze_sample=100000", connection=conn)
    duckdb.sql("SET enable_progress_bar=true", connection=conn)
    duckdb.sql("SET threads TO 3", connection=conn)
    duckdb.commit(conn)
    return conn


def update_src_question():
    all_target_answers = glob.glob(
        "./data/scrape_question_answer/**.csv"
    )
   

    all_tmp_df = []
    for x in tqdm(all_target_answers):
        try:
            tmp = pd.read_csv(x)
            all_tmp_df.append(tmp)
        except Exception as e1:
            print(x)

    target_df = pd.concat(all_tmp_df)
    target_df.drop_duplicates(subset=["qId", "aid"], inplace=True)
    target_df.dropna(subset=["qId", "aid"], inplace=True)
    target_df["qaId"] = target_df.apply(
        lambda x: str(x["qId"]) + str(int(x["aid"])), axis=1
    )
    target_df["aid"] = target_df["aid"].astype(int)
    downloaded_answer = pd.read_parquet(
        "./all_current_progress.parquet"
    )
    downloaded_answer["qaId"] = downloaded_answer.apply(
        lambda x: str(int(x["question_id"])) + str(int(x["answer_id"])), axis=1
    )
    new_qa_df = target_df[
        ~target_df["qaId"].isin(downloaded_answer["qaId"].unique().tolist())
    ]
    return new_qa_df

def update_task_and_deduplicate(conn):
    target_df = update_src_question()
    unique_data = duckdb.sql(
        """
        Select count (distinct (cast(question_id as BIGINT),  cast(answer_id as BIGINT)))
        FROM raw_zhihu_answers
        """,
        connection=conn,
    )
    logger.success(f"Unique answers : \n{unique_data}")
    target_df["qaId"] = target_df.apply(
        lambda x: str(x["qId"]) + str(int(x["aid"])), axis=1
    )

    downloaded_data = duckdb.sql(
        """
        Select concat(cast(question_id as BIGINT),  cast(answer_id as BIGINT)) as qaId
        FROM raw_zhihu_answers
        """,
        connection=conn,
    ).df()
    logger.success(f"Current downloaded answers : {len(downloaded_data)}")
    target_df = target_df[
        ~target_df["qaId"].isin(downloaded_data["qaId"].unique().tolist())
    ]
    logger.success(f"Current pending download task : {len(target_df)}")
    return target_df


'''
This process do not require parallelism as most of the time bottleneck is in 
the network IO. Process this step in machine with VPN can significantly increase
shadow socks 5 response speed. 
This process also handles the inserting of valid response into db. 
Currently we are using duckdb. 
'''
async def download_answers(config:Download_Answer_Config):
    """
    Parameters
    ----------
    parallel_req : parallel_req is the parallel request that we will wait for asynchronously
    save_interval : the number of data to accumulate before inserting into db
    
    Return
    ------
    None
    
    Output
    ------
    Create a duckdb file in ./db/zhihu.db
    Saves zhihu answers to table raw_zhihu_answers
    """

    '''
    Auto reruns the loop when current task list runs out.
    The reason :
    1. Only approximate 25% of shadow socks proxy returns valid response. 
    Shadow socks proxy changes every 10 minutes so we are uncertain of which proxy will
    be block or not accessible. 
    2. We assume there is a seperate running process producing more question-answer-url.
    Question-answer-url scraping is the most time consuming scraping process currently.
    By reruning the task list repeatedly in different order, we can hopefully get all 
    data eventually. 
    '''
    conn = init_db()
    while True:
        target_df = update_task_and_deduplicate(conn)
        # Shuffles the target task to ensure different request is forwarded to different proxy everytime.
        target_df = target_df.sample(frac=1)
        all_req_cor = []
        all_response = []
        total_data_added = 0
       
        complete_df = pd.DataFrame()
        timeout = httpx.Timeout(30.0, connect=3)
        limit = httpx.Limits(max_connections=30000, max_keepalive_connections=30000)
        '''
        This whitelist proxy is the one I tested to have produce at least 1 valid response previously.
        Use any free proxy list found online would work.
        '''
        with open(
            "./whitelist.json", "r"
        ) as f:
            whitelist_proxy_ip = json.load(f)
        all_whitelist_ip = list(whitelist_proxy_ip.keys())
        '''
        
        '''
        for id, (_, row) in enumerate(tqdm(target_df.iterrows())):
            try:
                qId = row["qId"]
                aId = row["aid"]
                question_title = row["question_title"]
                proxy_choice = all_whitelist_ip[id % len(all_whitelist_ip)]
                random_proxy = f"socks5://{ proxy_choice}"
                client = httpx.AsyncClient(
                    timeout=timeout,
                    limits=limit,
                    proxies={"https://": random_proxy},
                    verify=False,
                )
                # get answer content 
                corr = get_answer_content(
                    qid=qId,
                    aid=aId,
                    question_str=question_title,
                    req_client=client,
                    proxy=proxy_choice,
                )
                all_req_cor.append(corr)
                if len(all_req_cor) >= config.parallel_req:
                # Wait for next request to resolve to maintain active pending task
                # 
                    done, running_tasks = await asyncio.wait(
                        all_req_cor, return_when=asyncio.FIRST_COMPLETED
                    )
                    all_req_cor = list(running_tasks)
                    for task in done:
                        try:
                            result: Content_Data = await task
                            all_response.append(result)
                            if len(all_response) % 10 == 0:
                                logger.success(
                                    f"Received response sucessfully {len(all_response)}"
                                )

                        except Exception as e2:
                            logger.debug(f"E2 : {e2}")
                '''
                Convert raw json data to dataframe for ease of processing
                '''
                if len(all_response) >= config.save_interval:
                    tmp_df = pd.json_normalize(
                        [dataclasses.asdict(x) for x in all_response]
                    )
                    assert not tmp_df.empty
                    complete_df = pd.concat([complete_df, tmp_df])
                    all_response.clear()
            except Exception as e1:
                logger.debug(f"E1 {e1}")
            '''
            Save dataframe into db periodically.
            This process is intended to be a simple loading. Deduplication and other processing 
            will not be carry out here. 
            '''
            if len(complete_df) >= config.save_interval:
                print(
                    duckdb.sql(
                        """INSERT INTO raw_zhihu_answers SELECT * FROM complete_df""",
                        connection=conn,
                    )
                )
                print(duckdb.commit(conn))
                print(
                    duckdb.sql(
                        """ SELECT count(*) FROM raw_zhihu_answers""",
                        connection=conn,
                    )
                )
                total_data_added += len(complete_df)
                logger.success(f"Current added data : {total_data_added}")

                complete_df = pd.DataFrame()


if __name__ == "__main__":
    config = Scrape_Question_Config(batch_size=1, worker_num=6, num_scroll=50, headless=True)
    config_2 = Download_Answer_Config(parallel_req=10000, save_interval=200)
    asyncio.run(scrape_common_topics_async())
    asyncio.run(scrape_question_urls(config))
    asyncio.run(download_answers(config_2))
