from bs4 import BeautifulSoup as bs
from random import choice
from datetime import datetime, timedelta
from re import findall
from json import dumps
import aiohttp
from aiohttp.client_exceptions import ClientOSError, ServerDisconnectedError
from aiohttp.client_exceptions import ServerTimeoutError
from asyncio.exceptions import TimeoutError
import asyncio
import logging

logger = logging.getLogger(__name__)

url = "http://rsp.iseu.by/Raspisanie/TimeTable/umu.aspx"

weekdays = ('Понедельник',
            'Вторник',
            'Среда',
            'Четверг',
            'Пятница',
            'Суббота',
            'Воскресенье')


def parse(html: str, group: str, subgroup, parse_type):
    soup = bs(html, "html.parser")
    data = []
    if "Нет занятий" not in html:
        result = ""
        pre = soup.find("body").find("form").find("table", attrs={"id": "TT"})
        table = [i for i in
                 pre.find_all("tr", recursive=False)[3:]
                 if "row-separator" not in i["class"]]
        for row in table:
            data.append(list(filter(None, [ele.text.strip()
                                           for ele in row.find_all('td')])))
        last_time = ""
        special_key = "".join([choice(['!', '@', '#', '$',
                                       '%', '&', '^', '*',
                                       '+', '-']) for _ in range(8)])
        for i in data:
            if len(i) >= 3:
                if i[0].startswith(weekdays):
                    weekday, _date = i[0].split()
                    result += f"{special_key}<b>{weekday}</b> " \
                              f"<code>{_date}</code>\n\n<b>Группа</b>: " \
                              f"<i>{group}</i>\n" \
                              f"<b>Подгруппа</b>: " \
                              f"<i>{subgroup if subgroup else 'все'}</i>\n"
                    del i[0]
                if len(i) == 3:
                    i.insert(2, "")
                if len(i) == 2:
                    i.insert(1, "")
                    i.insert(2, "")
                teacher = " ".join(i[2].split()[-3:])
                del i[2]
                if last_time != i[0]:
                    last_time = i[0]
                    result += f"\n<b>Время</b> <i><u>{last_time}</u></i>:\n"
                del i[0]
                if "п/гр" in i[0] and isinstance(subgroup, int):
                    if f"{subgroup}п/гр" in i[0]:
                        i[0] = i[0].replace(f"{subgroup}п/гр", "")
                    else:
                        continue
                result += f"<blockquote>   Пара: {i[0]}\n" \
                          f"   Аудитория: {i[1].replace('ауд. ', '')}\n" \
                          f"   Преподаватель:\n   {teacher}</blockquote>\n"
        if "п/гр" in result:
            for i in set(findall(r"\d+п/гр", result)):
                result = result.replace(i, f" {i}")
        result = result.strip(special_key).split(special_key)

        for r in [result.index(k) for k in result if "Время" not in k]:
            del result[r]

        _tmp = []
        for i in [i.strip("\n") for i in result]:
            text = i
            regex = (
                        r"\n<b>Время</b> <i><u>.+</u></i>:"
                        r"(?!\n<blockquote>.+\n.+\n.+\n.+</blockquote>)\n?"
                    )
            for j in findall(regex, text):
                text = text.replace(j, "")
            _tmp.append(text)
        result = _tmp
        if parse_type == "full":
            return result
        elif isinstance(parse_type, datetime):
            date = parse_type.strftime("%d.%m.%Y")
            for i in result:
                if date in i:
                    return i
            return "Расписания, пока что, нет"

        elif isinstance(parse_type, int):
            return result[parse_type]
    else:
        return [f"<b>Группа</b>: <i>{'все' if not group else group}</i>\n"
                f"<b>Подгруппа</b>: <i>{subgroup if subgroup else 'все'}</i>"
                f"\nУРАААА!!!\nГУЛЯЕМ!"] * 7


async def get_data(session: aiohttp.ClientSession,
                   faculty: str,
                   learn_type: str,
                   course: int,
                   group: str,
                   subgroup,
                   view_type: str = "full"):
    data = {}

    pre = {
        "": "",
        "ddlFac": "",
        "ddlDep": "",
        "ddlCourse": "",
        "ddlGroup": "",
        "ddlWeek": "",
    }

    tmp = {
        "ddlFac": faculty,
        "ddlDep": learn_type,
        "ddlCourse": f"{course} курс",
        "ddlGroup": group,
        "ddlWeek":
            (
                datetime.now() - timedelta(days=datetime.now().weekday())
            ).strftime("%d.%m.%Y")
            if not view_type or not isinstance(view_type, datetime)
            else view_type.strftime("%d.%m.%Y")
    }

    for arg in pre:
        html = None
        while not html:
            try:
                async with session.post(url, data=data) as resp:
                    html = await resp.text()
                soup = bs(html, "html.parser")
                form = soup.find("body").find("form")

                if not form:
                    return

                for i in form.find_all(
                                "input",
                                {"type": "hidden"}
                                ):
                    data.update({i["name"]: i["value"]})

                if arg:
                    key = ""
                    data.update({"__EVENTTARGET": arg, arg: pre[arg]})
                    for k in form.find(
                                    "select",
                                    {"name": arg}
                                ).find_all("option"):
                        if k.text.strip() == tmp[arg]:
                            key = k["value"]
                            break
                    else:
                        return
                    data[arg] = key

            except (
                    ServerDisconnectedError,
                    ClientOSError,
                    ServerTimeoutError,
                    TimeoutError
                    ):
                await asyncio.sleep(2)

    data.update({"ShowTT": "Показать", "iframeheight": "400"})

    html = ""

    if not data["ddlWeek"]:
        return

    while not html:
        try:
            async with session.post(url, data=data) as resp:
                html = await resp.text()
        except (
                ServerDisconnectedError,
                ClientOSError,
                ServerTimeoutError,
                TimeoutError
                ):
            await asyncio.sleep(2)

    if view_type == "next" or view_type == "now":
        if view_type == "now":
            view_type = datetime.now()
        elif view_type == "next":
            view_type = datetime.now() + timedelta(days=1)
        if datetime.now().hour >= 15 and datetime.now().weekday() != 5:
            view_type += timedelta(days=1)
    elif isinstance(view_type, datetime):
        if view_type.weekday() == 0:
            view_type = "full"


    res = parse(html, group, subgroup, view_type)

    if not subgroup:
        subgroup = "All"
    return (faculty,
            learn_type,
            course,
            group,
            subgroup,
            data["ddlWeek"].split()[0], dumps(res))


async def generate_group_data(session: aiohttp.ClientSession,
                              fac: str,
                              lt: str,
                              cs: str):
    try:
        while True:
            dt = {}
            try:
                async with session.get(url) as resp:
                    _s = bs(await resp.text(), "html.parser")
                    for i in \
                            _s.find("select",
                                    attrs={"id": "ddlFac"}).find_all("option"):
                        if i.text.strip() == fac:
                            dt.update({'ddlFac': i["value"]})
                            break
                    dt.update({
                        '__VIEWSTATE': _s.find(
                            "input",
                            attrs={"type": "hidden", "name": "__VIEWSTATE"}
                        ).get("value"),
                        '__EVENTVALIDATION': _s.find(
                            "input",
                            attrs={"type": "hidden",
                                   "name": "__EVENTVALIDATION"}
                        ).get("value"),
                        '__EVENTTARGET': 'ddlCourse',
                        'ddlDep': lt
                    })
                    break
            except (ServerDisconnectedError,
                    ClientOSError,
                    ServerTimeoutError,
                    TimeoutError):

                logger.error("Connection error")
                logger.warning("Sleep on 2 sec...")
                await asyncio.sleep(2)

        while True:
            try:
                async with session.post(url, data=dt) as resp:
                    html = await resp.text()
                    _s = bs(html, "html.parser")
                    for i in _s.find("select",
                                     attrs={"id":
                                            "ddlCourse"}).find_all("option"):
                        if i.text.split()[0] == cs:
                            dt.update({"ddlCourse": i["value"]})
                            break
                    dt.update({
                        '__VIEWSTATE': _s.find(
                            "input",
                            attrs={"type": "hidden", "name": "__VIEWSTATE"}
                        ).get("value"),
                        '__EVENTVALIDATION': _s.find(
                            "input",
                            attrs={"type": "hidden",
                                   "name": "__EVENTVALIDATION"}
                        ).get("value"),
                        '__EVENTTARGET': 'ddlGroup'
                    })
                    break
            except (ServerDisconnectedError,
                    ClientOSError,
                    ServerTimeoutError,
                    TimeoutError):

                logger.error("Connection error")
                logger.warning("Sleep on 2 sec...")
                await asyncio.sleep(2)

        while True:
            try:
                async with session.post(url, data=dt) as resp:
                    soup = bs(await resp.text(), "html.parser")
                    break
            except (ServerDisconnectedError,
                    ClientOSError,
                    ServerTimeoutError,
                    TimeoutError):

                logger.error("Connection error")
                logger.warning("Sleep on 2 sec...")
                await asyncio.sleep(2)

        fac = soup.find("select",
                        attrs={
                            "id": "ddlFac"
                            }).find("option",
                                    attrs={"selected": "selected"
                                           }).text.strip()
        lt = soup.find("select",
                       attrs={
                                "id": "ddlDep"
                              }).find("option",
                                      attrs={"selected": "selected"
                                             }).text.strip()
        cs = int(cs)

        return [[(session, fac, lt, cs, i.text, j,
                  datetime.now()-timedelta(days=datetime.now().weekday())),
                 (session, fac, lt, cs, i.text, j,
                  datetime.now()+timedelta(days=7-datetime.now().weekday()))]
                for j in [None, 1, 2, 3]
                for i in soup.find("select", attrs={"id": "ddlGroup"
                                                    }).find_all("option")]
    except Exception:
        return


async def create_requests(session: aiohttp.ClientSession, fac: str):
    pld = {}
    tasks = []
    learn_types = ["2", "3"]

    logger.info("Trying create request...")

    while True:
        try:
            async with session.get(url) as resp:
                soup = bs(await resp.text(), "html.parser")
                pld = {
                    '__VIEWSTATE': soup.find(
                        "input",
                        attrs={"type": "hidden", "name": "__VIEWSTATE"}
                    ).get("value"),
                    '__EVENTVALIDATION': soup.find(
                        "input",
                        attrs={"type": "hidden", "name": "__EVENTVALIDATION"}
                    ).get("value"),
                    '__EVENTTARGET': 'ddlCourse'
                }
            for i in soup.find("select", attrs={"id":
                                                "ddlFac"}).find_all("option"):
                if i.text.strip() == fac:
                    pld.update({"ddlFac": i["value"]})
                    break
            break

        except (ServerDisconnectedError,
                ClientOSError,
                ServerTimeoutError,
                TimeoutError):

            logger.error("Connection error")
            logger.warning("Sleep on 2 sec...")
            await asyncio.sleep(2)

    while learn_types:
        try:
            if "ddlDep" not in pld:
                pld.update({"ddlDep": learn_types[0]})
            async with session.post(url, data=pld) as resp:
                soup = bs(await resp.text(), "html.parser")
                for i in soup.find("select",
                                   attrs={"id":
                                          "ddlCourse"}).find_all("option"):
                    tasks.append(generate_group_data(session,
                                                     fac,
                                                     learn_types[0],
                                                     i["value"]))
            del learn_types[0]

        except (ServerDisconnectedError,
                ClientOSError,
                ServerTimeoutError,
                TimeoutError):

            logger.error("Connection error")
            logger.warning("Sleep on 2 sec...")
            await asyncio.sleep(2)

    logger.info("Succeed generate pre-requests")
    logger.warning("Trying get group data")

    return sum(filter(None, await asyncio.gather(*tasks)), [])
