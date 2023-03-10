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
                                  async_playwright,
                                  Browser
                                  
                                  )
from playwright.sync_api import sync_playwright
from tqdm import tqdm
import playwright
from typing import Any
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
        headers = request.headers.copy()

        # if "https://www.zhihu.com/api/v5.1/topics/" in request.url:
        #     headers['sec-ch-ua'] = '"Chromium";v="110", "Not A(Brand";v="24", "Google Chrome";v="110"'
        #     headers["x-ab-pb"] = "ClIbAD8ARwC0AGkBagF0ATsCzALXAtgCtwPWBBEFUQWLBYwFngUxBusGJwd0CHkIYAn0CUkKawq+CkMLcQuHC40L1wvgC+UL5gtxDI8MrAzDDPgMEikAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACAA=="
        #     headers["x-zse-96"] = "2.0_hr1LT7N+0YVmQJGTcnJGa2VyGer+Fnm2SVpYL0S7nNm78BLKfm8iN6t9l6uic5dJ"
        #     headers["x-zst-81"] = "3_2.0VhnTj77m-qofgh3TxTnq2_Qq2LYuDhV80wSL7iUZQ6nxERY0m4fBJCHMiqHPD4S1hCS974e1DrNPAQLYlUefii7q26fp2L2ZKgSfnveCgrNOQwXTt_Fqsruy6unLcbO1NukyrAuOiRnxEL2ZZrxmDucmqhPXnXFMTAoTF6RhRuLPF_g9xUe0k6xYTqL_FUFM89SBcQc9EhLmNCCm_rNCnGLYPJXfaqO_jhL0rgcTveeu60YObUX9pDcmb7CVh9Ymlhpfk9L_CU2fwu3YfqNBbqomsDoBYcP0m039nCH9WqSmqBO0Ng3fDhHf6TX92AL_s0oCQJNOFuFMEcNMUbL9eir_HUtmOqfzK4eVsvoLTvXpQwVZkGxY2eN1kUXY89e8GwO9WhwKgqSLhhoGlU2LJAN9SbHM-BXMqCeYFJU86GL1fDXym7w0tGOq2hxsuhNB17e1QBNqyCeBwhHCGBHC"
        #     headers["cookie"] = "_zap=ea665014-a855-421b-8657-6aa2a6cc4869; _xsrf=1a9c9f79-5a12-4e9a-a7cd-8799bb81da5d; d_c0=AKDX6ZitbhaPTr72favEmBxE60LWzmLf1L4=|1678164604; Hm_lvt_98beee57fd2ef70ccdd5ca52b9740c49=1678164606; gdxidpyhxdE=ec41vjNt%2FP00I9fopJSNKSGXEoTzZ%5CR%5CC%2Bl%2FdzYpWe87tLGOKmMfcNN8Xyr8pjcr7Oj5kGXEzPCV5H9zwfUOW2%5CPQb0X%5C6RPC%5CYJAdRNg945loUjnzB8w59tPW7R3kbNUPupJH36UrLrLG18KrbgmgkQdwxaB73NAH%2Fvj7RY0xvWPUXo%3A1678166092763; YD00517437729195%3AWM_NI=qc72U%2BV9VFRrEOnuFSEvfl%2FT5AxGOV%2B%2FM4Xrms1CQnC%2FGqBTf7UeDwtWHlwgPI5Qq83dPwtl8uMQsjWmglNDGFnjLmasYOiLNd4jo37g9I7PsKg9%2BOhtiiHsUiN5QIrtNEo%3D; YD00517437729195%3AWM_NIKE=9ca17ae2e6ffcda170e2e6eed1bc79aab99a90d67fb2b88fa7c55b928b8e86d45c9aea9783fb50a68afe8ec62af0fea7c3b92ab3bdf9d3d82196e79aa7d17a8791a296c55987aaaa8dae60f79fb8a8ce5ee9f183d5db7aafadffa4d46681f18992c160a6e99b86c77db889888df66d9497fbb2f434f1bca098f768fcab83b2f26295b3a98dcd50859aa28bcf67a3ad97b0ec488ebcfc8bee39f5b9f7aab65cb195a9daaa45acaca78acb7997b782d6d443b2ac82a6ee37e2a3; YD00517437729195%3AWM_TID=cLJ2etwLEwdFVRVEBUKFKcrJP0CbEz3X; Hm_lpvt_98beee57fd2ef70ccdd5ca52b9740c49=1678165207; KLBRSID=fe0fceb358d671fa6cc33898c8c48b48|1678165207|1678164604; captcha_session_v2=2|1:0|10:1678165207|18:captcha_session_v2|88:WGJsbkw1ZTRva1JiN1BIbWt5d1pNVnQ4M3FLZ0xnNUxVYTBRaUNLdG1JNlptQ0NjVUtmOWlRMXV1dWd3N1c2aw==|c00fd69841f0e4955bf63a8232f122eb99ffc9b6b247f1a7e981a102341e2bd5"

        # logger.success(request.url)

        await route.continue_(headers=headers)

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


async def cancel_pop_up(page:Page):
    await page.wait_for_timeout(1000)
    await page.locator("body > div:nth-child(35) > div > div > div > div.Modal.Modal--default.signFlowModal > button > svg").click()

async def get_question_answer_urls_from_page(page: Page, topic_url: str, scrolldown=5):
    # await page.wait_for_timeout(np.random.randint(1000, 3500))

    await page.goto(topic_url)
    # await page.wait_for_timeout(np.random.randint(1000, 3500))

    await cancel_pop_up(page)
    # await page.wait_for_timeout(np.random.randint(1000, 3500))
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


async def weird_ritual(browser:Browser, device:Any):
    try:
            context = await browser.new_context(
            **device,
            java_script_enabled=False,
            screen={"width": 1920, "height": 1200}
        )
            page: Page = await context.new_page()
        
            await page.goto("https://www.zhihu.com/topic", timeout=3000)
            await context.close()

    except Exception as e1:
        logger.info(e1)

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
    print(relevent_hrefs)
    req_to_abort = ["jpeg", "png", "gif", "banners", "webp"]
    async with async_playwright() as p:
        device = p.devices["Desktop Firefox"]
        '''
        This is the weird ritual. 
        We need to start a browser with javascript disabled first. 
        1. Go to Zhihu and let it sit.
        2. Create a new context with javascript enabled. 
        3. Now this session can sroll down infinitely
        4. Currently we do not need to do the weird ritual everytime. 
        It seems like zhihu has registered us as normal users.
        
        '''
        browser = await p.firefox.launch(headless=headless, timeout=100000000)

        # await weird_ritual(browser, device)
        context = await browser.new_context(
        # **device,
        # java_script_enabled=True,
        # screen={"width": 1920, "height": 1200}
        )
        
        page: Page = await context.new_page()
        await page.route(
            "**",
            lambda route, request: asyncio.create_task(
                intercept_request(route, request, req_to_abort)
            ),
        )
        for topic_url in tqdm(relevent_hrefs):
            try:
                question_urls = await get_question_answer_urls_from_page(
                    page, topic_url,
                    scrolldown=20
                )
                
                print(question_urls)
                print(len(question_urls))
                print(len(set(question_urls)))
                for qId in question_urls:
                    qUrl = qId.replace("?write", "")

                    await page.goto(qUrl)
                    question_title_cor = await page.locator(
                        ".QuestionHeader-title"
                    ).all_inner_texts()
                    question_title = question_title_cor[0]

                    all_hrefs = await get_all_href(page.locator(".QuestionAnswers-answers"))
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
                        all_question_cor.append( get_answer_content(
                            qId, aId, question_title
                        ))
                    complete_content_data = await asyncio.gather(*all_question_cor)
                    content_data_dict = [dataclasses.asdict(x) for x in complete_content_data]
                    all_payloads.extend(content_data_dict)
                    logger.success(f"Received {len(content_data_dict)} answers from question : {question_title}")
            except Exception as e1:
                logger.error(e1)
            tmp_df = pd.json_normalize(all_payloads)
            print(tmp_df)
            tmp_df.to_csv("common_topics_zhihu.csv")
            logger.success(f"Saved {len(tmp_df)} answers to disk.")


if __name__ == "__main__":
    headless = False
    # scrape_people_roundtable()
    # end_to_end_auto_scrape()
    # scrape_round_tables()
    # asyncio.run(end_to_end_auto_scrape(headless))
    # asyncio.run(scrape_topics(headless))
    # a = requests.get("https://www.zhihu.com/api/v5.1/topics/19555513/feeds")
    # print(a.content)
    asyncio.run(end_to_end_auto_scrape_common_topics(headless))
