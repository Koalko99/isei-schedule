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
            date = datetime.strftime(parse_type, "%d.%m.%Y")
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
    data = {
        "__EVENTTARGET": "",
        "__EVENTARGUMENT": "",
        "__LASTFOCUS": "",
        "__VIEWSTATE": "",
        "__VIEWSTATEGENERATOR": "",
        "__EVENTVALIDATION": "",
        "ddlFac": "",
        "ddlDep": "",
        "ddlCourse": "",
        "ddlGroup": "",
        "ddlWeek": ""
    }
    pre_load = ""
    while True:
        dt = {}
        try:
            async with session.get(url) as resp:
                _s = bs(await resp.text(), "html.parser")
                fac = _s.find("select", attrs={"id": "ddlFac"})
                for i in fac.find_all("option"):
                    if i.text.strip() == faculty:
                        dt.update({"ddlFac": i["value"]})
                        break
                dep = _s.find("select", attrs={"id": "ddlDep"})
                for i in dep.find_all("option"):
                    if i.text.strip() == learn_type:
                        dt.update({"ddlDep": i["value"]})
                        break
                viewstate = _s.find("input",
                                    attrs={"type": "hidden",
                                           "name": "__VIEWSTATE"}
                                    ).get("value")
                eventvalidation = _s.find("input",
                                          attrs={
                                            "type": "hidden",
                                            "name": "__EVENTVALIDATION"
                                            }).get("value")

                dt.update({
                    '__VIEWSTATE': viewstate,
                    '__EVENTVALIDATION': eventvalidation,
                    '__EVENTTARGET': 'ddlCourse'
                })
                data = dt
                break
        except (ServerDisconnectedError,
                ClientOSError,
                ServerTimeoutError,
                TimeoutError):
            await asyncio.sleep(2)
    while True:
        try:
            async with session.post(url, data=data) as resp:
                html = await resp.text()
                _s = bs(html, "html.parser")
                for i in _s.find("select",
                                 attrs={"id": "ddlCourse"}).find_all("option"):
                    if int(i.text.split()[0]) == course:
                        data.update({"ddlCourse": i["value"]})
                        break
                data.update({
                            '__VIEWSTATE': _s.find(
                                "input",
                                attrs={"type": "hidden", "name": "__VIEWSTATE"}
                            ).get("value"),
                            '__EVENTVALIDATION': _s.find(
                                "input",
                                attrs={"type": "hidden",
                                       "name": "__EVENTVALIDATION"}
                            ).get("value"),
                            '__EVENTTARGET': 'ddlGroup'})
                break
        except (ServerDisconnectedError,
                ClientOSError,
                ServerTimeoutError,
                TimeoutError):
            await asyncio.sleep(2)
    while True:
        try:
            async with session.post(url, data=data) as resp:
                pre_load = bs(await resp.text(), "html.parser")
                break
        except (ServerDisconnectedError,
                ClientOSError,
                ServerTimeoutError,
                TimeoutError):
            await asyncio.sleep(2)
    data.update({"ShowTT": "Показать", "iframeheight": "400"})
    form = pre_load.find("body").find("form", attrs={"method": "post"})
    for input_block in form.find_all("input"):
        data.update({input_block.get("name"): input_block.get("value")})
        filtr = form.find("div", attrs={"class": "filter"})
    for faclt_data in filtr.find_all("div", recursive=False):
        for fdata in faclt_data.find_all("select"):
            method = fdata.get("name")
            temp = {option.text.strip(): option.get("value")
                    for option in fdata.find_all("option", recursive=False)}
            try:
                if method == "ddlFac":
                    data.update({method: temp[faculty]})
                elif method == "ddlDep":
                    data.update({method: temp[learn_type.lower()]})
                elif method == "ddlCourse":
                    data.update({method: temp[f"{course} курс"]})
                elif method == "ddlGroup":
                    try:
                        data.update({method: temp[group.upper()]})
                    except Exception:
                        html = ""
                        while not html:
                            post_data = {
                                '__VIEWSTATE': data['__VIEWSTATE'],
                                '__EVENTVALIDATION': data['__EVENTVALIDATION'],
                                '__EVENTTARGET': 'ddlGroup',
                                '__EVENTARGUMENT': '',
                                'ddlCourse': str(course)
                            }
                            try:
                                async with session.post(url,
                                                        data=post_data) \
                                                        as resp:
                                    html = await resp.text()
                            except (ServerDisconnectedError,
                                    ClientOSError,
                                    ServerTimeoutError,
                                    TimeoutError):
                                await asyncio.sleep(2)
                        pre_load = bs(html, "html.parser")
                        slct = pre_load.find("select",
                                             attrs={"name": "ddlGroup"})
                        for i in slct.find_all("option"):
                            if i.text == group:
                                viewstate = pre_load.find('input',
                                                          {'id': '__VIEWSTATE'
                                                           })['value']
                                ev = pre_load.find('input',
                                                   {'id': '__EVENTVALIDATION'
                                                    })['value']

                                data.update({
                                    method: i.get("value"),
                                    '__VIEWSTATE': viewstate,
                                    '__EVENTVALIDATION': ev
                                })
                                break
                elif method == "ddlWeek":
                    if not isinstance(view_type, datetime):
                        if datetime.strftime(
                                        datetime.now(), "%d.%m.%Y") in temp:
                            data.update({method:
                                         datetime.now().strftime(
                                             "%d.%m.%Y 0:00:00")})
                        else:
                            if (datetime.now().weekday() >= 5
                                    and datetime.now().hour >= 15):
                                next_week = (datetime.now() +
                                             timedelta(days=7 -
                                                       datetime.now().weekday()
                                                       ))
                                monday = (datetime.now() -
                                          timedelta(
                                              days=datetime.now().weekday()))
                                data.update({"ddlWeek":
                                             temp[next_week.strftime(
                                                 "%d.%m.%Y")]})
                            else:
                                data.update({"ddlWeek":
                                             temp[monday.strftime(
                                                                "%d.%m.%Y")]})
                    else:
                        data.update({method: view_type.strftime(
                                                        "%d.%m.%Y 0:00:00")})
                        view_type = "full"
            except Exception:
                return

    if view_type == "next" or view_type == "now":
        if view_type == "now":
            view_type = datetime.now()
        elif view_type == "next":
            view_type = datetime.now() + timedelta(days=1)
        if datetime.now().hour >= 15 and datetime.now().weekday() != 5:
            view_type += timedelta(days=1)

    html = ""
    while not html:
        try:
            async with session.post(url, data=data) as resp:
                html = await resp.text()
        except (ServerDisconnectedError,
                ClientOSError,
                ServerTimeoutError,
                TimeoutError):
            await asyncio.sleep(2)

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
            await asyncio.sleep(2)
    return sum(filter(None, await asyncio.gather(*tasks)), [])
