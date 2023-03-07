from __future__ import annotations

import asyncio
import dataclasses
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
from playwright.async_api import (Locator, Page, Request, Route,
                                  async_playwright)
from playwright.sync_api import sync_playwright
from tqdm import tqdm

"""
Change the logger level to debug for verbose
"""
logger.remove()
logger.add(sys.stderr, level="INFO")


@dataclass
class Content_Data:
    question_id: int
    answer_id: int
    author_id: str
    question_title: str
    content: str
    upvotes: str
    answer_creation_time: str


async def get_answer_content(qid: int, aid: int, question_str: str) -> str:
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
    headers = {
        "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 14_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1 Mobile/15E148 Safari/604.1",
        "Host": "www.zhihu.com",
    }
    url = f"https://www.zhihu.com/question/{qid}/answer/{aid}"
    logger.debug(f"start req {url}")
    # PROXIES = {
    # 'http://': 'socks5://127.0.0.1:9050',
    # 'https://': 'socks5://127.0.0.1:9050'
    # }
    async with httpx.AsyncClient(proxies=None) as client:
        response = await client.get(url, headers=headers)
    logger.debug(f"received response {url}")

    soup = BeautifulSoup(response.text, "html.parser")
    content = " ".join([p.text.strip() for p in soup.find_all("p")])
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
    return Content_Data(
        question_id=qid,
        answer_id=aid,
        author_id=author_id,
        question_title=question_str,
        content=content,
        upvotes=upvotes,
        answer_creation_time=answer_creation_time_content,
    )


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


"""
Scrape people from round table topics. Save a list of zhihu people profile url to csv
"""


# def scrape_people_roundtable():
#     headless = False
#     all_ppl_df = pd.DataFrame()
#     roundtable_topic_scrolldown = 20
#     with sync_playwright() as p:
#         browser = p.chromium.launch(headless=headless, timeout=60000)
#         page = browser.new_page()
#         page.goto("https://zhihu.com/roundtable")
#         # Scroll down roundtable topic to get more topic urls
#         for _ in range(roundtable_topic_scrolldown):
#             page.keyboard.down("End")
#             page.wait_for_timeout(1000)

#         hrefs = get_all_href(page)
#         relevent_hrefs = [x for x in hrefs if "https://www.zhihu.com/roundtable/" in x]
#         np.random.shuffle(relevent_hrefs)
#         # Earlier round table topic might not have started yet. The offset roundtable topic is arbitrary.

#         starting_offset = 4
#         for topic_url in tqdm(relevent_hrefs[starting_offset:]):
#             try:
#                 page.goto(topic_url)
#                 all_hrefs = get_all_href(page)
#                 people_urls = [x for x in all_hrefs if "/people/" in x]
#                 latest_people_id = pd.DataFrame({"people_id": people_urls})
#                 all_ppl_df = pd.concat([all_ppl_df, latest_people_id])
#             except Exception as e1:
#                 logger.error(e1)

#             all_ppl_df.to_csv("people.csv")


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
        # logger.success(request.url)

        await route.continue_()

    # if request.url == 'https://www.example.com/api':
    #     # Modify the request headers or body
    #     request.headers['Authorization'] = 'Bearer <access_token>'
    #     request.post_data = 'param1=value1&param2=value2'


"""
End to end auto scrape topics from round table
"""


async def end_to_end_auto_scrape(headless=True):
    pattern = r"/question/\d+/answer/\d+"
    all_payloads = []
    all_round_table_df = pd.read_csv("data/round_table_topics.csv")
    relevent_hrefs = all_round_table_df["round_table_topic_url"].tolist()
    np.random.shuffle(relevent_hrefs)
    req_to_abort = ["jpeg", "png", "gif", "banners", "webp"]
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless, timeout=10000)
        page: Page = await browser.new_page()
        await page.route(
            "**",
            lambda route, request: asyncio.create_task(
                intercept_request(route, request, req_to_abort)
            ),
        )

        # Earlier round table topic might not have started yet. The offset roundtable topic is arbitrary.
        starting_offset = 2
        for topic_url in tqdm(relevent_hrefs[starting_offset:]):
            try:
                question_urls = await get_questions(page, topic_url)
                for qId in question_urls:
                    qUrl = qId.replace("?write", "")

                    await page.goto(qUrl)
                    question_title_cor = await page.locator(
                        ".QuestionHeader-title"
                    ).all_inner_texts()
                    question_title = question_title_cor[0]

                    all_hrefs = await get_all_href(
                        page.locator(".QuestionAnswers-answers")
                    )
                    # search for all question-answer url
                    matches_question_answer_url = set(
                        [
                            s
                            for s in all_hrefs
                            if isinstance(s, str) and re.search(pattern, s)
                        ]
                    )

                    all_question_cor = []
                    for k in matches_question_answer_url:
                        elem = k.split("/")
                        qId = int(elem[-3])
                        aId = int(elem[-1])
                        all_question_cor.append(
                            get_answer_content(qId, aId, question_title)
                        )
                    complete_content_data = await asyncio.gather(*all_question_cor)
                    content_data_dict = [
                        dataclasses.asdict(x) for x in complete_content_data
                    ]
                    all_payloads.extend(content_data_dict)
                    logger.success(
                        f"Received {len(content_data_dict)} answers from question : {question_title}"
                    )
            except Exception as e1:
                logger.error(e1)
            tmp_df = pd.json_normalize(all_payloads)
            print(tmp_df)
            tmp_df.to_csv("zhihu.csv")
            logger.success(f"Saved {len(tmp_df)} answers to disk.")


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


async def scrape_topics(headless=True):
    output_dir = "./data"
    base_topic = [
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
    batch_size = 3
    batch_topics = []
    for i in range(0, len(base_topic), batch_size):
        batch_topics.append(base_topic[i : i + batch_size])
    scroll_down_num = 30
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless, timeout=60000)
        for batch in batch_topics:
            page_traverse_cor = []
            current_pages: List[(str, Page)] = []
            for topic in batch:
                page = await browser.new_page()
                page_traverse_cor.append(page.goto(f"https://zhihu.com/topics#{topic}"))
                current_pages.append((topic, page))
            await asyncio.gather(*page_traverse_cor)
            # await page.wait_for_timeout(2000)
            all_df = pd.DataFrame()
            skip_page_id = []

            for _ in tqdm(range(scroll_down_num)):
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
                all_df.to_csv(f"{output_dir}/common_topics.csv", header=False, mode="a")
            [await x.close() for _, x in current_pages]


async def get_question_answer_urls_from_page(page: Page, topic_url: str, scrolldown=5):
    await page.goto(topic_url)

    for _ in range(scrolldown):
        await page.keyboard.down("End")
        await page.wait_for_timeout(1000)

    all_hrefs = await get_all_href(page)
    question_urls = set(
        [
            x for x in all_hrefs if "/question/" in x and "/answer/" in x
        ]  # this will remove examples like https://zhuanlan.zhihu.com/p/336435588
    )
    return question_urls


"""
End to end auto scrape common topics
"""


async def end_to_end_auto_scrape_common_topics(headless=True):
    pattern = r"/question/\d+/answer/\d+"
    all_payloads = []
    all_round_table_df = pd.read_csv("data/common_topics.csv")
    relevent_hrefs = all_round_table_df["topic_urls"].tolist()

    # only crawl questions with top answers
    relevent_hrefs = [x + "/top-answers" for x in relevent_hrefs[0:1]]
    np.random.shuffle(relevent_hrefs)

    req_to_abort = ["jpeg", "png", "gif", "banners", "webp"]
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless, timeout=10000)
        page: Page = await browser.new_page()
        await page.route(
            "**",
            lambda route, request: asyncio.create_task(
                intercept_request(route, request, req_to_abort)
            ),
        )

        for topic_url in tqdm(relevent_hrefs):
            try:
                question_urls = await get_question_answer_urls_from_page(
                    page, topic_url
                )
                print(question_urls)
            #     for qId in question_urls:
            #         qUrl = qId.replace("?write", "")

            #         await page.goto(qUrl)
            #         question_title_cor = await page.locator(
            #             ".QuestionHeader-title"
            #         ).all_inner_texts()
            #         question_title = question_title_cor[0]

            #         all_hrefs = await get_all_href(page.locator(".QuestionAnswers-answers"))
            #         # search for all question-answer url
            #         matches_question_answer_url = set(
            #             [
            #                 s
            #                 for s in all_hrefs
            #                 if isinstance(s, str) and re.search(pattern, s)
            #             ]
            #         )

            #         all_question_cor = []
            #         for k in matches_question_answer_url:
            #             elem = k.split("/")
            #             qId = int(elem[-3])
            #             aId = int(elem[-1])
            #             all_question_cor.append( get_answer_content(
            #                 qId, aId, question_title
            #             ))
            #         complete_content_data = await asyncio.gather(*all_question_cor)
            #         content_data_dict = [dataclasses.asdict(x) for x in complete_content_data]
            #         all_payloads.extend(content_data_dict)
            #         logger.success(f"Received {len(content_data_dict)} answers from question : {question_title}")
            except Exception as e1:
                logger.error(e1)
            # tmp_df = pd.json_normalize(all_payloads)
            # print(tmp_df)
            # tmp_df.to_csv("common_topics_zhihu.csv")
            # logger.success(f"Saved {len(tmp_df)} answers to disk.")


if __name__ == "__main__":
    headless = False
    # scrape_people_roundtable()
    # end_to_end_auto_scrape()
    # scrape_round_tables()
    # asyncio.run(end_to_end_auto_scrape(headless))
    # asyncio.run(scrape_topics(headless))
    asyncio.run(end_to_end_auto_scrape_common_topics(headless))
