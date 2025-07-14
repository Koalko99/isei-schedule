import aiohttp
from aiohttp.client_exceptions import ServerDisconnectedError, ClientOSError
from aiohttp.client_exceptions import ServerTimeoutError
import asyncio
from asyncio.exceptions import TimeoutError
from bs4 import BeautifulSoup as bs
from random import choice
from datetime import datetime, timedelta
from json import dumps
import logging

logger = logging.getLogger(__name__)

teacher_url = "http://rsp.iseu.by/Raspisanie/TimeTable/umuteachers.aspx"

teachers = {}


async def _get_teacher_data(
                            session: aiohttp.ClientSession,
                            teacher_id: int,
                            date: str = None
                            ):
    data = {}
    pre = {
            "": "",
            "ddlWeek": f"{date} 0:00:00" if date else "",
            "DropDownList1": teacher_id,
            "cbShowDraftForTeacher": "on"
            }

    if not pre["ddlWeek"]:
        last_monday = (datetime.now() -
                       timedelta(days=datetime.now().weekday()))
        pre["ddlWeek"] = last_monday.strftime("%d.%m.%Y 0:00:00")

    def validate(x):
        if len(x) == 2:
            if not x[1]:
                del x[1]
        return x

    for arg in pre:
        html = None
        while not html:
            data.update({"__EVENTTARGET": arg, arg: pre[arg]})
            try:
                async with session.post(teacher_url, data=data) as resp:
                    html = await resp.text()
                soup = bs(html, "html.parser")
                form = soup.find("body").find("form")
                if not form:
                    return
                for i in form.find_all("input", {"type": "hidden"}):
                    data.update({i["name"]: i["value"]})
                await asyncio.sleep(2)
            except (ServerDisconnectedError,
                    ClientOSError,
                    ServerTimeoutError,
                    TimeoutError):

                logger.error("Connection error")
                logger.warning("Sleep on 3 sec...")
                await asyncio.sleep(3)

    data.update({"Show": "Показать", "__EVENTTARGET": ""})
    html = None
    while not html:
        try:
            async with session.post(teacher_url, data=data) as resp:
                html = await resp.text()
            soup = bs(html, "html.parser")
            main_table = soup.find("body").find("table", attrs={"id": "TT"})
            table = [
                validate([ele.text.strip() for ele in i.find_all('td')])
                for i in main_table.find_all("tr", recursive=False)[3:]
                if "row-separator" not in i.attrs.get(list(i.attrs)[0], "")
            ]
            return teacher_id, pre["ddlWeek"], table
        except (ServerDisconnectedError,
                ClientOSError,
                ServerTimeoutError,
                TimeoutError):

            logger.error("Connection error")
            logger.warning("Sleep on 3 sec...")
            await asyncio.sleep(3)


async def pre_data(session: aiohttp.ClientSession, date: str = None):
    global teachers
    html = None
    tasks = []
    res = []

    if not date:
        date = (datetime.now() -
                timedelta(days=datetime.now().weekday())).strftime("%d.%m.%Y")

    while not html:
        try:
            logger.warning("Trying generate pre-data")
            async with session.get(teacher_url) as resp:
                html = await resp.text()
            soup = bs(html, "html.parser")
            dropdown = soup.find("body").find("form").find(
                "select", {"name": "DropDownList1"}
            )
            for row in dropdown.find_all("option")[1:]:
                teachers.update({row["value"]:
                                 " ".join(row.text.split()[:3])})
                tasks.append((session, row["value"], date))
            shift = 30
            logger.warning("Parsing teacher schedule...")

            if date not in html:
                logger.error(f"Schedule for teacher on {date} in not exist")
                return []

            for i in range(0, len(tasks), shift):
                while True:
                    try:
                        for r in await asyncio.gather(
                                    *[_get_teacher_data(*r)
                                        for r in tasks[i:i+shift]]):

                            res.append((teachers[r[0]], *r))
                        await asyncio.sleep(3)
                        break
                    except Exception:
                        logger.error("Connection error")
                        logger.warning("Sleep on 3 sec...")
                        await asyncio.sleep(3)
            logger.info(f"Succeed parse teacher info on {date}")
            return res

        except (ServerDisconnectedError,
                ClientOSError,
                ServerTimeoutError,
                TimeoutError):

            logger.error("Connection error")
            logger.warning("Sleep on 3 sec...")
            await asyncio.sleep(3)


def t_parser(data):
    res = ""
    special_key = "".join([choice(['!', '@', '#', '$',
                                   '%', '&', '^', '*',
                                   '+', '-']) for _ in range(8)])
    for row in data[3]:
        if len(row[0].split()) > 1 and len(row) > 1:
            weekday, date = row[0].split()
            res += f"{special_key}<b>{weekday}</b> <code>{date}</code>" \
                   f"\n\n<b>Преподаватель:</b> <i>{data[0]}</i>\n"
            row = row[1:]
        elif len(row) == 1:
            res += f"{special_key}Расписания на " \
                   f"<code>{row[0].split()[1]}</code> нет"
            continue
        # if len(row) == 3:
        #     row.insert(1, "")
        #     row.insert(3, "")
        # elif len(row) == 4:
        #     row.insert(3, "")
        # elif len(row) == 2:
        #     row.insert(1, "")
        #     row.insert(2, "")
        #     row.insert(3, "")
        res += f"\n<b>Время</b> <i><u>{row[0]}</u></i>:\n"
        room = ""
        corp = ""
        if len(row[4].split(" ауд.")) > 1:
            corp, room = row[4].split(" ауд.")
            corp = corp[1:-1]
        else:
            room = row[4]
        res += f"<blockquote>   Дисциплина: {row[2]}\n" \
               f"   Контингент: {row[1]}\n" \
               f"   Подгруппа: {row[3] if row[3] else 'все'}\n" \
               f"   Аудитория: {room}\n" \
               f"   Адрес: {corp}\n</blockquote>"
    res = res.lstrip(special_key)
    res = res.split(special_key)
    return data[1], data[0], data[2].split()[0], dumps(res)
