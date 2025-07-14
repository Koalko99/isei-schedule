import aiohttp
from aiohttp.client_exceptions import ClientOSError, ServerDisconnectedError
from aiohttp.client_exceptions import ServerTimeoutError
import asyncio
from asyncio.exceptions import TimeoutError
from bs4 import BeautifulSoup as bs
from time import perf_counter
from re import findall, compile
import threading
from students import create_requests, get_data, url, weekdays
from teachers import pre_data, t_parser
from datetime import datetime, timedelta
import aiosqlite
from json import loads
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters.callback_data import CallbackData
from aiogram.filters import Command
from aiogram.enums import ParseMode
import logging
import sys
import warnings
from dotenv import load_dotenv
from os import getenv

load_dotenv()

logger = logging.getLogger(__name__)

data_bank = {}
bot = Bot(token=getenv("API_KEY"))
dp = Dispatcher()

terminate = threading.Event()

COLORS = {
    'DEBUG':    '\033[36m',
    'INFO':     '\033[37m',
    'WARNING':  '\033[33m',
    'ERROR':    '\033[31m',
    'CRITICAL': '\033[1;31m'
}
RESET = '\033[0m'

log_format = "[%(asctime)s] [%(levelname)-s]: %(message)s"
date_format = "%Y-%m-%d %H:%M:%S"

main_keyboard = types.ReplyKeyboardMarkup(
                    keyboard=[
                        [types.KeyboardButton(text="Сегодня")],
                        [types.KeyboardButton(text="Завтра")],
                        [types.KeyboardButton(text="По дню недели")],
                        [types.KeyboardButton(text="Следующая неделя")],
                        [types.KeyboardButton(text="Профиль")]
                        ], resize_keyboard=True)


pattern = compile(rf"({'|'.join(weekdays)})")


class ColorFormatter(logging.Formatter):
    def format(self, record):
        color = COLORS.get(record.levelname, '')
        message = super().format(record)
        return f"{color}{message}{RESET}"


class CallbackFactory(CallbackData, prefix="c"):
    action: str
    value: str = None


console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(ColorFormatter(log_format, datefmt=date_format))

file_handler = logging.FileHandler("schedule.log", encoding="utf-8")
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(logging.Formatter(log_format, datefmt=date_format))

logging.basicConfig(
    level=logging.INFO,
    handlers=[console_handler, file_handler]
)

logger = logging.getLogger(__name__)


async def sql_start():
    global db
    db = await aiosqlite.connect('database.db')
    await db.execute('''
                     CREATE TABLE IF NOT EXISTS students(
                                                        id INTEGER UNIQUE,
                                                        username STRING,
                                                        faculty STRING,
                                                        learn_type STRING,
                                                        course INTEGER,
                                                        user_group STRING,
                                                        subgroup STRING,
                                                        sended STRING
                                                        )
                     ''')

    await db.execute('''
                     CREATE TABLE IF NOT EXISTS s_info(
                                                        faculty STRING,
                                                        learn_type STRING,
                                                        course INTEGER,
                                                        u_group STRING,
                                                        subgroup STRING,
                                                        week STRING,
                                                        data STRING
                                                        )
                     ''')

    await db.execute('''
                     CREATE TABLE IF NOT EXISTS teachers(
                                                        id INTEGER,
                                                        tg_id INTEGER,
                                                        username STRING,
                                                        sended STRING
                                                        )
                     ''')

    await db.execute('''
                     CREATE TABLE IF NOT EXISTS t_info(
                                                       id INTEGER,
                                                       name STRING,
                                                       week STRING,
                                                       data STRING
                                                       )
                     ''')
    await db.commit()


async def get_teacher_key(ID: int):
    teacher_key = None
    async with db.execute("""
                          SELECT id
                          FROM teachers
                          WHERE `tg_id` = ?""",
                          (ID,)) as cursor:
        teacher_key = await cursor.fetchone()

    if teacher_key:
        teacher_key = teacher_key[0]
    else:
        teacher_key = None

    return teacher_key


async def schedule(ID):
    last_sended = ""

    teacher_key = await get_teacher_key(ID)
    str_to_find = ""
    text_to_send = ""

    if not teacher_key:
        res = []

        if len(res) < 6:
            return
        async with db.execute(
            """
            SELECT faculty,
                   learn_type,
                   course,
                   user_group,
                   subgroup

            FROM students
            WHERE id = ?
            """,
                (ID, )) as cursor:

            res = list(await cursor.fetchone())
            res.append(
                (datetime.now() +
                 timedelta(days=7-datetime.now().weekday())
                 if (datetime.now().hour >= 15
                     and datetime.now().weekday() >= 5)
                 else datetime.now() -
                    timedelta(days=datetime.now().weekday()
                              )).strftime("%d.%m.%Y"))
            res = tuple(filter(None, tuple(res)))
            await asyncio.sleep(5)

        async with db.execute("""
                              SELECT data
                              FROM s_info WHERE `faculty` = ?
                              AND `learn_type` = ?
                              AND `course` = ?
                              AND `u_group` = ?
                              AND `subgroup` = ?
                              AND `week` = ?
                              """, (*res[0:4],
                                    str(res[-2]),
                                    res[-1])) as cursor:
            data = await cursor.fetchone()
            data = loads((data)[0])

            if data:
                if datetime.now().weekday() <= 5:
                    if datetime.now().hour <= 17:
                        str_to_find = datetime.now().strftime("%d.%m.%Y")
                    else:
                        str_to_find = (datetime.now() +
                                       timedelta(
                                           days=1
                                           if datetime.now().weekday() != 5
                                           else 2)).strftime("%d.%m.%Y")

                    for i in data:
                        if str_to_find in i:
                            text_to_send = i
                    if not text_to_send:
                        text_to_send = None
                else:
                    text_to_send = data[0]

        async with db.execute(
                    """
                    SELECT sended FROM students WHERE id = ?
                    """,
                    (ID,)
                ) as cursor:
            last_sended = (await cursor.fetchone())[0]

        if last_sended != text_to_send and text_to_send:

            logger.info(f"Send {ID} student data on {str_to_find}")

            await bot.send_message(ID,
                                   text_to_send,
                                   reply_markup=main_keyboard,
                                   parse_mode=ParseMode.HTML)

            await db.execute("""
                             UPDATE students SET `sended` = ? WHERE `id` = ?
                             """, (text_to_send, ID))
            await db.commit()

    else:
        now = datetime.now()

        if now.hour >= 15 and now.weekday() >= 5:
            monday = now + timedelta(days=7 - now.weekday())
        else:
            monday = now - timedelta(days=now.weekday())

        week = monday.strftime("%d.%m.%Y")

        async with db.execute(
                        """
                        SELECT data
                        FROM t_info
                        WHERE id = ? AND week = ?
                        """,
                        (teacher_key, week)
                    ) as cursor:
            info = loads((await cursor.fetchone())[0])

        async with db.execute("""
                              SELECT sended
                              FROM teachers
                              WHERE `id` = ?
                              """,
                              (teacher_key,)) as cursor:

            last_sended = (await cursor.fetchone())[0]

        if info:
            if datetime.now().weekday() <= 5:
                if datetime.now().hour <= 17:
                    str_to_find = datetime.now().strftime("%d.%m.%Y")
                else:
                    str_to_find = (datetime.now() +
                                   timedelta(
                                       days=1
                                       if datetime.now().weekday() != 5
                                       else 2)).strftime("%d.%m.%Y")

                for i in info:
                    if str_to_find in i:
                        text_to_send = i
                if not text_to_send:
                    text_to_send = None
            else:
                text_to_send = info[0]

            if "</code> нет" in text_to_send:
                text_to_send = None

        if last_sended != text_to_send and text_to_send:

            logger.info(f"Send {ID} student data on {str_to_find}")

            await bot.send_message(ID,
                                   text_to_send,
                                   reply_markup=main_keyboard,
                                   parse_mode=ParseMode.HTML)
            await db.execute("""
                             UPDATE teachers
                             SET `sended` = ?
                             WHERE `tg_id` = ?
                             """,
                             (text_to_send, ID))
            await db.commit()


async def create_data_bank():
    global data_bank
    _tfac = []
    start = perf_counter()

    logger.info("Create session")

    async with aiohttp.ClientSession(
                            connector=aiohttp.TCPConnector(limit=300)
                            ) as session:

        while True:
            try:
                async with session.get(url,
                                       timeout=aiohttp.ClientTimeout(
                                            total=5)) as resp:
                    soup = bs(await resp.text(), "html.parser")
                    ddl_fac = soup.find("select", attrs={"name": "ddlFac"})
                    for _fac in ddl_fac.find_all("option"):
                        _tfac.append(_fac.text.strip())
                    break

            except (ServerDisconnectedError,
                    ClientOSError,
                    ServerTimeoutError,
                    TimeoutError):

                logger.error("Site is down")
                await asyncio.sleep(2)

        logger.warning("Create requests for parsing student schedule...")

        requests = sum(
                    sum(
                        list(
                            filter(None,
                                   await asyncio.gather(*[
                                       create_requests(session, fac)
                                       for fac in _tfac]))
                            ),
                        []),
                    []
                    )

        logger.warning("Get student schedule...")

        result = list(
                    filter(None,
                           await asyncio.gather(
                               *[get_data(*r) for r in requests]
                                ))
                    )

        logger.info("Succeed collect student schedule")

        logger.warning("Getting teacher schedule...")

        teacher_data = list(
                        map(
                            t_parser,
                            filter(
                                None,
                                await pre_data(session)
                                )
                            )
                        )

        teacher_data.extend(
            list(
                map(
                    t_parser,
                    filter(
                        None,
                        await pre_data(
                            session,
                            (
                                datetime.now() + timedelta(
                                    days=7 - datetime.now().weekday()
                                )
                            ).strftime("%d.%m.%Y")
                        )
                    )
                )
            )
        )

        logger.info("Succeed collect teacher schedule")
        end = perf_counter()

        logger.info("Update data in database")

        await db.execute("""
                         DELETE FROM s_info
                         """)
        await db.execute("""
                         DELETE FROM t_info
                         """)
        await asyncio.gather(*[
                        db.execute(
                            """
                            INSERT INTO s_info VALUES (?, ?, ?, ?, ?, ?, ?)
                            """, row) for row in result]
                            )
        await asyncio.gather(*[
                        db.execute(
                            """
                            INSERT INTO t_info VALUES (?, ?, ?, ?)
                            """, row) for row in teacher_data])
        await db.commit()
        logger.info(f"Total time: {end-start:.2f} sec")
        logger.info("Data updated")


@dp.message(Command("start"))
async def start(mess: types.Message):

    logger.info(f"ID {mess.from_user.id} start talking")

    async with db.execute("""
                          SELECT 1
                          FROM students
                          WHERE id = ?
                          AND EXISTS (
                              SELECT 1 FROM teachers WHERE tg_id = ?
                          )
                          """,
                          (mess.from_user.id,
                           mess.from_user.id)) as cursor:

        res = await cursor.fetchone()
        if res:
            await mess.answer(f"Привет, <i><b>{mess.from_user.first_name}"
                              f"</b></i>\nТы уже пользуешься ботом",
                              reply_markup=main_keyboard,
                              parse_mode=ParseMode.HTML)
        else:
            await mess.answer(f"Привет, <i><b>"
                              f"{mess.from_user.first_name}</b></i>\n"
                              f"Хочешь без задержек получать своё расписание?",
                              reply_markup=types.InlineKeyboardMarkup(
                                  inline_keyboard=[
                                      [types.InlineKeyboardButton(
                                          text="Да, давай",
                                          callback_data="signup")]
                                    ]), parse_mode=ParseMode.HTML)


@dp.callback_query(F.data == "signup")
async def signup(callback: types.CallbackQuery):
    await callback.answer()

    logger.warning(f"ID {callback.from_user.id} trying to registered")

    await db.execute("""
                     DELETE FROM students WHERE id = ?
                     """,
                     (callback.from_user.id,))
    await db.execute("""
                     DELETE FROM teachers WHERE tg_id = ?
                     """,
                     (callback.from_user.id,))
    await db.commit()

    await callback.message.delete()

    _msg = await callback.message.answer(
        "Минуту...",
        reply_markup=types.ReplyKeyboardRemove()
    )

    await _msg.delete()

    await callback.message.answer(
        "Вы студент или преподаватель?",
        reply_markup=types.InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    types.InlineKeyboardButton(
                        text="Студент",
                        callback_data=CallbackFactory(
                            action="student",
                            value="1"
                        ).pack()
                    ),
                    types.InlineKeyboardButton(
                        text="Преподаватель",
                        callback_data="page_0"
                    )
                ]
            ]
        )
    )


async def get_teacher_keybord(page: int):
    teachers = {}
    async with db.execute("""SELECT DISTINCT
                             name,
                             id
                             FROM t_info""") as cursor:
        teachers.update({name: key for name, key in await cursor.fetchall()})

    start = page * 10
    end = start + 10

    def ceil(x):
        if x % 1 == 0:
            return int(x)
        else:
            return int(x) + 1

    pre_keyboard = sorted(teachers.items(), key=lambda x: x[0])
    pages = ceil(len(pre_keyboard) / 10)

    pre_keyboard = [
        [types.InlineKeyboardButton(text=name, callback_data=f"teacher_{key}")]
        for name, key in pre_keyboard[start:end]
    ]

    left = (
        types.InlineKeyboardButton(
            text="⬅️",
            callback_data=f"page_{page - 1}"
        ) if page > 0 else
        types.InlineKeyboardButton(
            text=" ",
            callback_data="noop"
        )
    )

    center = types.InlineKeyboardButton(
        text=f"{page + 1}/{pages}",
        callback_data="noop"
    )

    right = (
        types.InlineKeyboardButton(
            text="➡️",
            callback_data=f"page_{page + 1}"
        ) if end < len(teachers) else
        types.InlineKeyboardButton(
            text=" ",
            callback_data="noop"
        )
    )

    pre_keyboard.append([left, center, right])

    keyboard = types.InlineKeyboardMarkup(inline_keyboard=pre_keyboard,
                                          row_width=1)

    return keyboard


@dp.callback_query(F.data.startswith("page_"))
async def page(callback: types.CallbackQuery):
    await callback.answer()

    p = int(callback.data.split("_")[1])

    logger.warning(f"ID {callback.from_user.id} open page {p+1}")

    keyboard = await get_teacher_keybord(p)

    await callback.message.edit_text("Выберите своё ФИО:",
                                     reply_markup=keyboard)


@dp.callback_query(F.data.startswith("delete"))
async def delete(callback: types.CallbackQuery):
    await callback.answer()

    if len(callback.data.split("_")) == 1:

        logger.warning(F"ID {callback.from_user.id} trying to delete account")

        await callback.message.edit_text(
            "Вы точно хотите "
            "удалить свой аккаунт?",
            reply_markup=types.InlineKeyboardMarkup(
                                inline_keyboard=[[
                                    types.InlineKeyboardButton(
                                        text="Да",
                                        callback_data="delete_yes"),

                                    types.InlineKeyboardButton(
                                        text="Нет",
                                        callback_data="delete_no")
                                                ]]
                                            )
                                        )
    else:
        _, ans = callback.data.split("_")
        if ans == "yes":

            logger.error(f"ID {callback.from_user.id} delete account")

            await db.execute("""
                             DELETE FROM students WHERE id = ?
                             """, (callback.from_user.id,))
            await db.execute("""
                             DELETE FROM teachers WHERE tg_id = ?
                             """, (callback.from_user.id,))
            await db.commit()

            await callback.message.answer(
                "Ваш аккаунт успешно удалён",
                reply_markup=types.ReplyKeyboardRemove())
        else:

            logger.info(f"ID {callback.from_user.id} "
                        "canceled account deletion")

            await callback.message.answer(
                "Удаление отменено",
                reply_markup=main_keyboard
            )


@dp.callback_query(F.data.startswith("teacher_"))
async def teacher(callback: types.CallbackQuery):
    await callback.answer()

    key = int(callback.data.split("_")[1])

    username = None
    if callback.from_user.username:
        username = "@"+callback.from_user.username
    await db.execute("""
                     INSERT INTO teachers VALUES (?, ?, ?, ?)
                     """,
                     (key,
                      callback.from_user.id,
                      username,
                      None))
    await db.commit()
    await callback.message.delete()
    await callback.message.answer("Теперь Вам будет автоматически "
                                  "приходить Ваше расписание",
                                  reply_markup=main_keyboard)
    if callback.from_user.username:
        logger.info(f"ID {callback.from_user.id}"
                    f"(@{callback.from_user.username}) "
                    f"registered as teacher({key})")
    else:
        logger.info(f"ID {callback.from_user.id} "
                    f"registered as teacher({key})")


@dp.callback_query(CallbackFactory.filter(F.action.contains("student")))
async def student(callback: types.CallbackQuery,
                  callback_data: CallbackFactory):

    await callback.answer()

    if callback_data.action == "student":
        async with db.execute("""
                              SELECT DISTINCT faculty
                              FROM s_info
                              """) as cursor:
            await callback.message.edit_text(
                    "Хорошо, выбери свой факультет",
                    reply_markup=types.InlineKeyboardMarkup(
                        inline_keyboard=[
                            [
                                types.InlineKeyboardButton(
                                    text=i,
                                    callback_data=CallbackFactory(
                                        action="student_1",
                                        value=str(j)
                                    ).pack()
                                )
                            ]
                            for j, i in enumerate(
                                            sorted(
                                                sum(await cursor.fetchall(),
                                                    ())
                                                )
                                            )
                        ]
                    )
                )

    elif callback_data.action == "student_1":
        async with db.execute("""
                              SELECT DISTINCT faculty
                              FROM s_info
                              """) as cursor:

            fac = sum(await cursor.fetchall(), ())[int(callback_data.value)]

        username = None
        if callback.from_user.username:
            username = "@"+callback.from_user.username

        await db.execute("""
                         INSERT INTO students
                         VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                         """,
                         (callback.from_user.id,
                          username
                          if callback.from_user.username else None,
                          fac, None, None, None, None, None))
        await db.commit()

        async with db.execute("""
                              SELECT DISTINCT learn_type
                              FROM s_info
                              WHERE `faculty` = ?
                              """,
                              (fac,)) as cursor:
            await callback.message.edit_text(
                "Отлично, теперь выбери свою форму обучения",
                reply_markup=types.InlineKeyboardMarkup(
                    inline_keyboard=[
                        [types.InlineKeyboardButton(
                            text=i,
                            callback_data=CallbackFactory(
                                action="student_2",
                                value=i).pack()
                            )] for i in sum(await cursor.fetchall(), ())]))

    elif callback_data.action == "student_2":
        await db.execute("""
                         UPDATE students
                         SET `learn_type` = ?
                         WHERE `id` = ?
                         """,
                         (callback_data.value,
                          callback.from_user.id))
        await db.commit()

        async with db.execute("""
                              SELECT DISTINCT course
                              FROM s_info
                              WHERE faculty = (
                                    SELECT faculty
                                    FROM students
                                    WHERE id = ?
                                )
                              AND learn_type = (
                                    SELECT learn_type
                                    FROM students
                                    WHERE id = ?
                                )
                              """,
                              (callback.from_user.id,
                               callback.from_user.id)) as cursor:

            await callback.message.edit_text(
                "Замечательно, теперь выбери свой курс",
                reply_markup=types.InlineKeyboardMarkup(
                    inline_keyboard=[
                        [types.InlineKeyboardButton(
                            text=str(i),
                            callback_data=CallbackFactory(
                                action="student_3",
                                value=str(i)).pack()
                            )] for i in sum(await cursor.fetchall(), ())]))

    elif callback_data.action == "student_3":
        await db.execute("""
                         UPDATE students
                         SET `course` = ?
                         WHERE `id` = ?
                         """,
                         (int(callback_data.value),
                          callback.from_user.id))
        await db.commit()

        async with db.execute("""
                              SELECT DISTINCT u_group
                              FROM s_info
                              WHERE faculty = (
                                    SELECT faculty
                                    FROM students
                                    WHERE id = ?)
                              AND course  = (
                                    SELECT course
                                    FROM students
                                    WHERE id = ?)
                              AND learn_type = (
                                    SELECT learn_type
                                    FROM students
                                    WHERE id = ?)
                              """, (
                                    callback.from_user.id,
                                    callback.from_user.id,
                                    callback.from_user.id)) as cursor:
            await callback.message.edit_text(
                "Хорошо, теперь выбери свою группу",
                reply_markup=types.InlineKeyboardMarkup(
                    inline_keyboard=[
                        [types.InlineKeyboardButton(
                            text=i,
                            callback_data=CallbackFactory(
                                action="student_4",
                                value=i).pack()
                            )] for i in sum(await cursor.fetchall(), ())]
                        )
                    )

    elif callback_data.action == "student_4":
        await db.execute("""
                         UPDATE students
                         SET `user_group` = ?
                         WHERE `id` = ?
                         """,
                         (callback_data.value, callback.from_user.id))
        await db.commit()

        await callback.message.edit_text(
            "Теперь выбери свою подгруппу",
            reply_markup=types.InlineKeyboardMarkup(
                inline_keyboard=[
                    [types.InlineKeyboardButton(
                        text=i,
                        callback_data=CallbackFactory(
                            action="student_5",
                            value=i).pack()
                        )] for i in ["Все", "1", "2", "3"]]
                    )
            )

    elif callback_data.action == "student_5":

        if callback_data.value == "Все":
            callback_data.value = "All"
        else:
            callback_data.value = int(callback_data.value)

        await db.execute("""
                         UPDATE students
                         SET `subgroup` = ?
                         WHERE `id` = ?
                         """,
                         (callback_data.value, callback.from_user.id))
        await db.commit()

        if callback.from_user.username:
            logger.info(f"ID {callback.from_user.id}"
                        f"(@{callback.from_user.username}) "
                        f"registered as student")
        else:
            logger.info(f"ID {callback.from_user.id} "
                        f"registered as student")

        await callback.message.delete()
        await callback.message.answer("Теперь тебе будет "
                                      "автоматически отправляться "
                                      "расписание твоих пар",
                                      reply_markup=main_keyboard)


@dp.message(F.text == "Профиль")
async def profile(mess: types.Message):

    logger.warning(f"ID {mess.from_user.id} view profile")

    teacher_key = await get_teacher_key(mess.from_user.id)
    if not teacher_key:
        async with db.execute("""
                              SELECT faculty,
                                     learn_type,
                                     course,
                                     user_group,
                                     subgroup
                              FROM students
                              WHERE `id` = ?
                              """,
                              (mess.from_user.id,)) as cursor:

            row = await cursor.fetchone()

            faculty = row[0]
            learn_type = row[1]
            course = row[2]
            user_group = row[3]
            subgroup = row[4]

            if subgroup == "All":
                subgroup = "Все"
            await mess.answer(
                f"<b>Профиль</b>: <i>{mess.from_user.first_name}</i>\n"
                f"<b>Факультет</b>: <i>{faculty}</i>\n"
                f"<b>Форма обучения</b>: <i>{learn_type}</i>\n"
                f"<b>Курс</b>: <i>{course}</i>\n"
                f"<b>Группа</b>: <i>{user_group}</i>\n"
                f"<b>Подгруппа</b>: <i>{subgroup}</i>",
                parse_mode=ParseMode.HTML,
                reply_markup=types.InlineKeyboardMarkup(inline_keyboard=[
                    [types.InlineKeyboardButton(
                        text="Изменить",
                        callback_data="signup"),
                     types.InlineKeyboardButton(
                        text="Удалить",
                        callback_data="delete")]
                ])
            )
    else:
        async with db.execute("""
                              SELECT name
                              FROM t_info
                              WHERE `id` = ?
                              """,
                              (teacher_key, )) as cursor:

            teacher_name = (await cursor.fetchone())[0]

        await mess.answer(
            f"<b>Профиль</b>: <i>{mess.from_user.first_name}</i>\n"
            f"<b>Преподаватель (Вы)</b>: <i>{teacher_name}</i>",
            parse_mode=ParseMode.HTML,
            reply_markup=types.InlineKeyboardMarkup(
                inline_keyboard=[
                    [
                        types.InlineKeyboardButton(
                            text="Изменить",
                            callback_data="signup"),
                        types.InlineKeyboardButton(
                            text="Удалить",
                            callback_data="delete")
                    ]
                ]
            )
        )


@dp.message(F.text == "Сегодня")
async def today(mess: types.Message):
    teacher_key = await get_teacher_key(mess.from_user.id)

    logger.info(f"ID {mess.from_user.id} looks at the schedule for today")

    week = (datetime.now() -
            timedelta(days=datetime.now().weekday())
            ).strftime("%d.%m.%Y")

    if not teacher_key:
        async with db.execute("""
                              SELECT faculty,
                                     learn_type,
                                     course,
                                     user_group,
                                     subgroup
                              FROM students
                              WHERE id = ?""",
                              (mess.from_user.id, )) as cursor:

            res = list(await cursor.fetchone())
            res.append(week)
            res = tuple(res)

        async with db.execute("""
                              SELECT data
                              FROM s_info
                              WHERE `faculty` = ?
                              AND `learn_type` = ?
                              AND `course` = ?
                              AND `u_group` = ?
                              AND `subgroup` = ?
                              AND `week` = ?
                              """,
                              (*res[0:4], str(res[-2]), res[-1])) as cursor:
            data = await cursor.fetchone()
            data = loads(data[0])

    else:
        async with db.execute("""
                              SELECT data
                              FROM t_info
                              WHERE `id` = ?
                              AND `week` = ?
                              """,
                              (teacher_key, week)) as cursor:
            data = await cursor.fetchone()
            data = loads(data[0])

    str_to_find = datetime.now().strftime("%d.%m.%Y")
    text_to_send = ""

    for i in data:
        if str_to_find in i:
            text_to_send = i

    if not text_to_send:
        text_to_send = (f"Извините, но расписания на "
                        f"<code>{str_to_find}</code> нет")

    await mess.answer(text_to_send,
                      reply_markup=main_keyboard,
                      parse_mode=ParseMode.HTML)


@dp.message(F.text == "Завтра")
async def next_day(mess: types.Message):
    teacher_key = await get_teacher_key(mess.from_user.id)

    logger.info(f"ID {mess.from_user.id} looks at the schedule for next day")

    if datetime.now().weekday() != 6:
        week = (datetime.now() -
                timedelta(
                    days=datetime.now().weekday())
                ).strftime("%d.%m.%Y")
    else:
        week = (datetime.now() +
                timedelta(
                    days=7-datetime.now().weekday())
                ).strftime("%d.%m.%Y")

    if not teacher_key:
        async with db.execute("""
                              SELECT faculty,
                                     learn_type,
                                     course,
                                     user_group,
                                     subgroup
                              FROM students WHERE id = ?
                              """,
                              (mess.from_user.id, )) as cursor:

            res = list(await cursor.fetchone())
            res.append(week)
            res = tuple(res)

        async with db.execute("""
                              SELECT data
                              FROM s_info
                              WHERE `faculty` = ?
                              AND `learn_type` = ?
                              AND `course` = ?
                              AND `u_group` = ?
                              AND `subgroup` = ?
                              AND `week` = ?
                              """,
                              (*res[0:4], str(res[-2]), res[-1])) as cursor:

            data = await cursor.fetchone()
            data = loads(data[0])
    else:
        async with db.execute("""
                              SELECT data
                              FROM t_info
                              WHERE `id` = ?
                              AND `week` = ?
                              """,
                              (teacher_key, week)) as cursor:

            data = await cursor.fetchone()
            data = loads(data[0])

    str_to_find = (datetime.now() + timedelta(days=1)).strftime("%d.%m.%Y")
    text_to_send = ""

    for i in data:
        if str_to_find in i:
            text_to_send = i

    if not text_to_send:
        text_to_send = (f"Извините, но расписания на "
                        f"<code>{str_to_find}</code> нет")

    await mess.answer(text_to_send,
                      reply_markup=main_keyboard,
                      parse_mode=ParseMode.HTML)


@dp.callback_query(F.data.contains("week"))
async def some_day(callback: types.CallbackQuery):
    await callback.answer()
    teacher_key = await get_teacher_key(callback.from_user.id)

    logger.info(f"ID {callback.from_user.id} looks at "
                f"the schedule for some day")

    week = ""

    if callback.data.startswith("first"):
        week = (datetime.now() -
                timedelta(
                    days=datetime.now().weekday())
                ).strftime("%d.%m.%Y")

    elif callback.data.startswith("second"):
        week = (datetime.now() +
                timedelta(
                    days=7-datetime.now().weekday())
                ).strftime("%d.%m.%Y")

    if not teacher_key:

        async with db.execute("""
                              SELECT faculty,
                                     learn_type,
                                     course,
                                     user_group,
                                     subgroup

                              FROM students
                              WHERE `id` = ?
                              """,
                              (callback.from_user.id, )) as cursor:
            res = list(await cursor.fetchone())

        res.append(week)

        res = tuple(res)

        async with db.execute("""
                              SELECT data
                              FROM s_info
                              WHERE `faculty` = ?
                              AND `learn_type` = ?
                              AND `course` = ?
                              AND `u_group` = ?
                              AND `subgroup` = ?
                              AND `week` = ?
                              """,
                              res) as cursor:

            data = loads((await cursor.fetchone())[0])

    else:

        async with db.execute("""
                              SELECT data
                              FROM t_info
                              WHERE `id` = ?
                              AND `week` = ?
                              """,
                              (teacher_key, week)
                              ) as cursor:

            data = loads((await cursor.fetchone())[0])

    text_to_send = ""
    for d in data:
        if callback.data.split("_")[-1] in d:
            text_to_send = d
            break

    await callback.message.delete()
    await callback.message.answer(text_to_send,
                                  reply_markup=main_keyboard,
                                  parse_mode=ParseMode.HTML)


@dp.message(lambda message:
            message.text.capitalize() in ['По дню недели',
                                          'Следующая неделя']
            )
async def pre_mess_some_day(mess: types.Message):
    teacher_key = await get_teacher_key(mess.from_user.id)
    if not teacher_key:

        async with db.execute("""
                              SELECT faculty,
                                     learn_type,
                                     course,
                                     user_group,
                                     subgroup

                              FROM students
                              WHERE id = ?
                              """,
                              (mess.from_user.id, )) as cursor:

            res = list(await cursor.fetchone())

            if mess.text == "По дню недели":
                res.append((datetime.now() -
                            timedelta(days=datetime.now().weekday())
                            ).strftime("%d.%m.%Y"))

            elif mess.text == "Следующая неделя":
                res.append((datetime.now() +
                            timedelta(days=7-datetime.now().weekday())
                            ).strftime("%d.%m.%Y"))
            res = tuple(res)

        async with db.execute("""
                              SELECT data
                              FROM s_info
                              WHERE `faculty` = ?
                              AND `learn_type` = ?
                              AND `course` = ?
                              AND `u_group` = ?
                              AND `subgroup` = ?
                              AND `week` = ?
                              """,
                              res) as cursor:

            data = loads((await cursor.fetchone())[0])

    else:

        if mess.text == "По дню недели":
            week = (datetime.now() -
                    timedelta(days=datetime.now().weekday())
                    ).strftime("%d.%m.%Y")

        elif mess.text == "Следующая неделя":
            week = (datetime.now() +
                    timedelta(days=7-datetime.now().weekday())
                    ).strftime("%d.%m.%Y")

        async with db.execute("""
                              SELECT data
                              FROM t_info
                              WHERE `id` = ?
                              AND `week` = ?
                              """,
                              (teacher_key, week)) as cursor:

            data = loads((await cursor.fetchone())[0])

    callback_week = ("first_week"
                     if mess.text == "По дню недели"
                     else "second_week")

    pre_keyboard = []

    for day in data:
        matches = findall(pattern, day)
        if matches:
            weekday = matches[0]
            pre_keyboard.append([
                types.InlineKeyboardButton(
                    text=weekday,
                    callback_data=f"{callback_week}_{weekday}"
                )
            ])

    if pre_keyboard:

        await mess.answer(
            "Выбери день недели",
            reply_markup=types.InlineKeyboardMarkup(
                inline_keyboard=pre_keyboard
            )
        )
    else:
        await mess.answer(
            (f"Расписания на "
             f"{'эту' if 'first' in callback_week else 'следующую'} "
             f"неделю нет"))


async def parse_data():
    counter = 0
    while not terminate.is_set():
        await create_data_bank()
        while not terminate.is_set() and counter < 300:
            await asyncio.sleep(1)
            counter += 1
        counter = 0


async def notify():
    counter = 0
    while not terminate.is_set():
        logger.info("Sending scheduled data...")
        async with db.execute("""
                              SELECT id
                              AS user_id
                              FROM students
                              UNION ALL
                              SELECT tg_id
                              AS user_id
                              FROM teachers
                              """) as cursor:
            tasks = [schedule(i) for i in sum(await cursor.fetchall(), ())]
        await asyncio.gather(*tasks)
        logger.info("End of sending")
        while not terminate.is_set() and counter < 300:
            await asyncio.sleep(1)
            counter += 1
        counter = 0


def thread():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    async def runner():
        try:
            await parse_data()
        except asyncio.CancelledError:
            pass
        finally:
            await loop.shutdown_asyncgens()
            loop.stop()

    loop.create_task(runner())
    try:
        loop.run_forever()
    finally:
        loop.close()


async def main():
    try:
        await sql_start()
        async with db.execute("""
                              SELECT
                                (SELECT COUNT(*)
                                    FROM s_info) +
                                (SELECT COUNT(*)
                                    FROM t_info)
                              AS total_count
                              """) as cursor:

            if not (await cursor.fetchone())[0]:
                logger.info("Parse data...")
                await create_data_bank()
        logger.info('Start bot')

        threading.Thread(target=thread, daemon=True).start()
        asyncio.create_task(notify())
        await dp.start_polling(bot, skip_updates=True)

    except KeyboardInterrupt:
        terminate.set()
        loop = asyncio.get_running_loop()
        tasks = [t for t in asyncio.all_tasks(loop)
                 if t is not asyncio.current_task()]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

        await dp.stop_polling()
        logger.info("Bot stopped")

    finally:
        if db:
            await db.close()
            logger.info("Database closed")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Goodbye")

    if sys.platform.startswith("win"):
        import asyncio.proactor_events
        warnings.filterwarnings("ignore",
                                category=ResourceWarning,
                                module="asyncio")

        old_del = asyncio.proactor_events._ProactorBasePipeTransport.__del__

        def safe_del(self):
            try:
                old_del(self)
            except RuntimeError as e:
                if str(e) != "Event loop is closed":
                    raise

        asyncio.proactor_events._ProactorBasePipeTransport.__del__ = safe_del
