#!/usr/bin/env python3
"""Добавление игрока в players.txt по заявке (issue) с сайта.

Текст заявки — недоверенный ввод: берём из него только тег из допустимых символов,
проверяем через API, что такой игрок существует, и дописываем тег в players.txt.
Результат печатается строками key=value для $GITHUB_OUTPUT: result, tag, name.
result: added | already | notfound | invalid | full | maintenance
"""
import os, re, sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import update_stats as u  # noqa: E402

PLAYERS_FILE = os.environ.get("PLAYERS_FILE", "players.txt")
MAX_TRACKED = u.env_num("BS_MAX_TRACKED", 300)
TAG_CHARS = "0289PYLQGRJCUV"  # символы, из которых состоят теги Brawl Stars


def find_tag(text):
    """Первый тег вида #XXXX в тексте; буква O считается нулём."""
    for m in re.finditer(r"#\s*([0-9A-Za-z]{3,14})", text or ""):
        t = u.norm_tag(m.group(1))
        if 3 <= len(t) <= 14 and all(c in TAG_CHARS for c in t):
            return t
    return None


def safe_name(name):
    """Ник для ответа в заявке: без переносов, обратных кавычек и @-упоминаний."""
    return re.sub(r"[`@\r\n\t<>]", "", str(name or ""))[:30].strip() or "без ника"


def out(**kv):
    for k, v in kv.items():
        print(f"{k}={v}")


def main():
    tag = find_tag(os.environ.get("ISSUE_TITLE", "")) or find_tag(os.environ.get("ISSUE_BODY", ""))
    if not tag:
        return out(result="invalid", tag="", name="")
    tracked = u.read_tags(PLAYERS_FILE)
    if tag in tracked:
        return out(result="already", tag=tag, name="")
    if len(tracked) >= MAX_TRACKED:
        return out(result="full", tag=tag, name="")
    key = os.environ.get("BS_API_KEY", "").strip()
    if not key:
        raise SystemExit("Нет ключа BS_API_KEY.")
    try:
        prof = u.api(u.player_path(tag), key)
    except u.Maintenance:
        return out(result="maintenance", tag=tag, name="")
    if not prof:
        return out(result="notfound", tag=tag, name="")
    with open(PLAYERS_FILE, "a+", encoding="utf-8") as f:
        f.seek(0)
        text = f.read()
        if text and not text.endswith("\n"):
            f.write("\n")
        f.write(f"#{tag}\n")
    out(result="added", tag=tag, name=safe_name(prof.get("name")))


if __name__ == "__main__":
    main()
