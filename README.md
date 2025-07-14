# 📅 Schedule Parser & Telegram Bot

The project combines the ISEI schedule parser and a telegram bot that sends the schedule. The parser and the bot work in parallel: each in its own asynchronous stream. The project is designed for ISEI teachers and students who want their schedule to be always available.

---

## 🚀 Features

- Asynchronous schedule parser and Telegram bot working in parallel
- Supports both teacher and student accounts
- Automatic dumping of schedule from the university website
- Navigation by days and weeks with inline keyboards
- User-friendly interface with profile management
- Always available, even when the official site is down

---

## 🎯 Motivation

The original website was often slow or inaccessible due to high demand.  
This project was created to **offload the schedule data** and make it **readable anytime** via a Telegram interface.

---

## 🔧 Requirements

- Python 3.9+
- [aiogram](https://docs.aiogram.dev/)
- [aiosqlite](https://github.com/omnilib/aiosqlite)
- [beautifulsoup4](https://pypi.org/project/beautifulsoup4/)

**Install all dependencies with:**

```bash
pip install -r requirements.txt
```
## ⚙️ Running the Project
### 1. Clone the repository:
```bash
git clone https://github.com/Koalko99/isei-schedule.git
cd isei-schedule
```
### 2. Create a `.env` file and set your Telegram bot token:
```env
API_KEY=your_token
```
### 3. Run the project:
```bash
python main.py
```

## 📬 Contact

If you have any questions, suggestions or ideas for cooperation — <u>contact me</u>:

- *Telegram:* [@Koalko101](https://t.me/Koalko101)
- *Email:* *koalko99@gmail.com*