#!/usr/bin/env python3
"""Сбор статистики ранкеда Brawl Stars (запускается GitHub Actions 4 раза в сутки).

Через официальный API (прокси RoyaleAPI) опрашивает топ-игроков 16 стран, затем их соперников
и союзников по ранкеду, и считает за скользящие 7 дней по матчам уровня Легенда+:
  * винрейт, популярность и тир каждого бойца, силу в режимах;
  * текущий пул карт ранкеда и сильных на каждой карте;
  * результаты пар «боец против бойца» и связки (пары и тройки в одной команде);
  * разбор каждого бойца и признаки аномальности.
Параллельно копит такую же статистику по всем рангам — для личного разбора игроков из players.txt.

Состояние (дневные сводки, истории игроков) — в папке --state; итог — meta.json, meta_all.json,
players/<ТЕГ>.json. Нужен только Python 3 без сторонних библиотек. Ключ API — в переменной BS_API_KEY.
"""
import argparse, concurrent.futures as cf, datetime as dt, hashlib, http.client, json, os, re, shutil, threading, time
import urllib.error, urllib.parse, urllib.request

API = os.environ.get("BS_API_URL", "https://bsproxy.royaleapi.dev/v1")
BRAWLAPI = "https://api.brawlapi.com/v1"
RU_WIKI = "https://brawlstars.fandom.com/ru/api.php"
UA = "draft-stats/1.0 (+https://github.com/znakcentrru/draft)"

COUNTRIES = ["global", "ru", "ua", "kz", "by", "us", "br", "mx", "de", "fr", "es", "it", "tr", "pl", "kr", "jp"]
MODES = {"gemGrab": "G", "brawlBall": "B", "heist": "H", "bounty": "Y", "knockout": "K", "hotZone": "Z"}
CLASS = {"Damage Dealer": "Д", "Tank": "Т", "Marksman": "М", "Artillery": "А",
         "Controller": "К", "Assassin": "У", "Support": "П"}
WINDOW_DAYS = 7       # за сколько дней считать статистику (старые дни выпадают сами)
POOL_DAYS = 7         # карта считается в пуле ранкеда, если на ней играли за эти дни
# В матчах ранкеда поле brawler.trophies — номер ранга: 1–3 Бронза, 4–6 Серебро, 7–9 Золото,
# 10–12 Алмаз, 13–15 Мифик, 16–18 Легенда, 19–21 Мастер, 22 Про.
RANK_NAMES = ["Бронза", "Серебро", "Золото", "Алмаз", "Мифик", "Легенда", "Мастер"]
def env_num(name, default, cast=int):
    """Число из переменной окружения; пусто или не число — значение по умолчанию (поля запуска вводятся руками)."""
    try:
        return cast(os.environ.get(name) or default)
    except ValueError:
        print(f"::warning::{name}={os.environ.get(name)!r} — не число, беру {default}.")
        return cast(default)


MIN_RANK = env_num("BS_MIN_RANK", 16)                   # Легенда I и выше
CRAWL_RANK = MIN_RANK - 3                               # соперников опрашиваем начиная с ранга на ступень ниже
DAYS_DIR = "days_r%d" % MIN_RANK                         # сводки отдельно для каждого порога ранга
DAYS_ALL = "days_all"                                    # те же сводки по всем рангам — для личного разбора игроков


def rank_name(r):
    r = max(1, int(r))  # порог 0 (BS_MIN_RANK=0) — это «от самого низкого ранга»
    if r >= 22:
        return "Про"
    return f"{RANK_NAMES[(r - 1) // 3]} {'I' * ((r - 1) % 3 + 1)}"


def valid_rank(r):
    """Номер ранга из brawler.trophies в матче ранкеда или None, если значение не похоже на ранг."""
    return r if isinstance(r, int) and 1 <= r <= 22 else None


def norm(name):
    return re.sub(r"[^a-z0-9]", "", name.lower())


ERRS, ERR_BODY = {}, {}
LOCK = threading.Lock()  # счётчики журнала обновляются из нескольких потоков


def count_err(k, body=None):
    with LOCK:
        ERRS[k] = ERRS.get(k, 0) + 1
        if body is not None:
            ERR_BODY.setdefault(k, body[:200])


class Maintenance(Exception):
    pass


def http_json(url, headers=None, tries=3):
    h = {"User-Agent": UA, "Accept": "application/json"}
    h.update(headers or {})
    for i in range(tries):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers=h), timeout=30) as r:
                return json.load(r)
        except urllib.error.HTTPError as e:
            body = e.read()[:300].decode("utf-8", "replace")
            if "/battlelog" in url:
                count_err(f"HTTP {e.code}", body)
            if e.code == 503 and "inMaintenance" in body:
                raise Maintenance()
            if e.code in (403, 404):
                if e.code == 403 and "accessDenied" in body and url.startswith(API):
                    raise SystemExit("Ключ API не подходит: " + body +
                                     "\nПроверьте секрет BS_API_KEY и что в ключе указан IP 45.79.218.79.")
                return None
            if e.code == 429 or e.code >= 500:
                time.sleep(2 * (i + 1)); continue
            return None
        # URLError и обрывы связи (RemoteDisconnected, ConnectionResetError, IncompleteRead, SSL),
        # а также битый ответ (ValueError: JSON, кодировка) — повторяем, а не роняем весь сбор
        except (OSError, http.client.HTTPException, ValueError) as e:
            if "/battlelog" in url:
                count_err(type(e).__name__ + ": " + str(e)[:80])
            time.sleep(2 * (i + 1))
    return None


def api(path, key):
    return http_json(API + path, {"Authorization": "Bearer " + key})


# ---------- сбор матчей ----------
def collect_players(key):
    tags = set()
    for c in COUNTRIES:
        d = api(f"/rankings/{c}/players?limit=200", key)
        for p in (d or {}).get("items", []):
            tags.add(p["tag"])
    return sorted(tags)


def is_ranked(b):
    t = (b.get("battle", {}).get("type") or "")
    return t.lower() in ("soloranked", "teamranked")


def parse_battle(owner, item):
    """Возвращает (день, ключ, режим, карта, победители, проигравшие, теги участников, средний ранг) или None."""
    if not is_ranked(item):
        return None
    ev, bt = item.get("event") or {}, item["battle"]
    mode = MODES.get(ev.get("mode") or bt.get("mode"))
    mp = ev.get("map")
    teams = bt.get("teams")
    if not mode or not mp or not teams or len(teams) != 2 or bt.get("result") not in ("victory", "defeat"):
        return None
    mine = 0 if any(p.get("tag") == owner for p in teams[0]) else 1
    if not any(p.get("tag") == owner for p in teams[mine]):
        return None
    win = mine if bt["result"] == "victory" else 1 - mine
    names = [[(p.get("brawler") or {}).get("name") for p in tm] for tm in teams]
    t = item.get("battleTime") or ""  # 20261103T101530.000Z
    if len(t) < 15 or any(not n for team in names for n in team):
        return None  # неполная запись боя
    tags = sorted(p.get("tag", "") for tm in teams for p in tm)
    day = f"{t[0:4]}-{t[4:6]}-{t[6:8]}"
    key = hashlib.sha1((t + mp + "".join(tags)).encode()).hexdigest()[:12]
    team_ranks = [[valid_rank((p.get("brawler") or {}).get("trophies")) for p in tm] for tm in teams]
    ranks = [r for tm in team_ranks for r in tm if r]
    avg = sum(ranks) / len(ranks) if len(ranks) >= 4 else None
    return (day, key, mode, mp, names[win], names[1 - win], [x for x in tags if x and x != owner], avg,
            (team_ranks[win], team_ranks[1 - win]))


DIAG = {"logs_ok": 0, "logs_fail": 0, "types": {}, "sample": None}
TAG_FORMATS = [lambda t: "%23" + t, lambda t: "%2523" + t, lambda t: t]
TAG_FMT = TAG_FORMATS[0]


def player_path(tag, suffix=""):
    """Путь к игроку в API в том формате тега, который принимает прокси (см. pick_tag_format)."""
    return "/players/" + TAG_FMT(tag.lstrip("#")) + suffix


def status(url, key):
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Authorization": "Bearer " + key})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.status
    except urllib.error.HTTPError as e:
        return e.code
    except Exception as e:
        return type(e).__name__


def pick_tag_format(key, tags):
    """Прокси по-разному обрабатывает «#» в теге — пробуем варианты и берём рабочий."""
    global TAG_FMT
    report = []
    for tag in tags[:25]:  # часть историй может быть временно недоступна — пробуем несколько игроков
        if status(f"{API}/players/%23{tag.lstrip('#')}/battlelog", key) == 200:
            TAG_FMT = TAG_FORMATS[0]
            print("Формат тега: %23")
            return True
    for tag in tags[:3]:
        t = tag.lstrip("#")
        for i, f in enumerate(TAG_FORMATS):
            code = status(f"{API}/players/{f(t)}/battlelog", key)
            report.append(f"{['%23', '%2523', 'без #'][i]}: {code}")
            if code == 200:
                TAG_FMT = f
                print("Формат тега:", ["%23", "%2523", "без #"][i])
                return True
    print("Ни один формат тега не подошёл:", "; ".join(report))
    t = tags[0].lstrip("#")
    for path in (f"/players/%23{t}", f"/players/%23{t}/battlelog", "/events/rotation", "/brawlers", "/rankings/global/players?limit=1"):
        print(f"  проверка {path.replace(t, 'TAG')}: {status(API + path, key)}")
    return False


def fetch_battles(key, tags, workers=10, deadline=None):
    out = []
    def one(tag):
        if deadline and time.time() > deadline:
            with LOCK:
                DIAG["skipped"] = DIAG.get("skipped", 0) + 1
            return []
        d = api(player_path(tag, "/battlelog"), key)
        if d is None:
            with LOCK:
                DIAG["logs_fail"] += 1
            return []
        with LOCK:
            DIAG["logs_ok"] += 1
            diag_battles(d)
        return [r for r in (parse_battle(tag, it) for it in d.get("items", [])) if r]
    with cf.ThreadPoolExecutor(workers) as ex:
        for res in ex.map(one, tags):
            out.extend(res)
    return out


def diag_battles(d):
    """Типы боёв и поля игроков — для журнала (вызывать под LOCK)."""
    for it in d.get("items", []):
        bt, ev = it.get("battle", {}), it.get("event") or {}
        k = f'{bt.get("type")}|{ev.get("mode") or bt.get("mode")}'
        DIAG["types"][k] = DIAG["types"].get(k, 0) + 1
        if str(bt.get("type")).lower() == "soloranked":
            for team in bt.get("teams") or []:
                for pl in team:
                    tr = (pl.get("brawler") or {}).get("trophies")
                    DIAG.setdefault("rank_hist", {})[tr] = DIAG.setdefault("rank_hist", {}).get(tr, 0) + 1
                    if "player_keys" not in DIAG:
                        DIAG["player_keys"] = sorted(pl.keys())
                        DIAG["brawler_keys"] = sorted((pl.get("brawler") or {}).keys())
        if DIAG["sample"] is None and "rank" in str(bt.get("type", "")).lower():
            DIAG["sample"] = {"event": ev, "battle": {x: y for x, y in bt.items() if x not in ("teams", "players", "starPlayer")},
                              "teamsShape": [len(t) for t in bt.get("teams") or []]}


def print_diag():
    print(f"Историй боёв: скачано {DIAG['logs_ok']}, не скачано {DIAG['logs_fail']}, не успели {DIAG.get('skipped', 0)}")
    print("Ошибки при скачивании историй:", json.dumps(ERRS, ensure_ascii=False), json.dumps(ERR_BODY, ensure_ascii=False))
    print("Типы боёв (тип|режим: сколько):")
    for k, v in sorted(DIAG["types"].items(), key=lambda kv: -kv[1])[:25]:
        print(f"  {k}: {v}")
    print("Пример боя с 'rank' в типе:", json.dumps(DIAG["sample"], ensure_ascii=False))
    rh = DIAG.get("rank_hist", {})
    print("Поля игрока в бою:", DIAG.get("player_keys"), "поля бравлера:", DIAG.get("brawler_keys"))
    print("brawler.trophies в soloRanked (значение: сколько):",
          ", ".join(f"{k}: {v}" for k, v in sorted(rh.items(), key=lambda kv: (kv[0] is None, kv[0] if isinstance(kv[0], int) else 0))[:60]))


def crawl(key, seeds, max_logs, deadline):
    """Опрашиваем топ-игроков, потом их соперников и союзников по ранкеду, потом их — и так далее.
    У игроков, которые играют реже топов, 25 последних боёв растягиваются на несколько дней,
    так набираются матчи за всю последнюю неделю."""
    visited, queue, battles, rnd = set(), list(seeds), [], 0
    while queue and len(visited) < max_logs and time.time() < deadline:
        rnd += 1
        todo = [t for t in dict.fromkeys(queue) if t not in visited]
        size = min(4000, max_logs - len(visited))
        batch, rest = todo[:size], todo[size:]
        if not batch:
            break
        visited.update(batch)
        res = fetch_battles(key, batch, deadline=deadline)
        battles.extend(res)
        found = {t for b in res if b[7] is not None and b[7] >= CRAWL_RANK for t in b[6]} - visited
        hi = sum(1 for b in res if b[7] is not None and b[7] >= MIN_RANK)
        # сначала только что найденные игроки сильных матчей, затем те, кто не поместился в этот круг
        queue = list(found) + rest
        print(f"Круг {rnd}: опрошено {len(batch)}, матчей ранкеда {len(res)}, из них {rank_name(MIN_RANK)}+ {hi}, "
              f"новых игроков для обхода {len(found)}")
    return battles, len(visited)


# ---------- дневные сводки ----------
def load(path, default):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return default


def save(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def add(d, k, win):
    g = d.setdefault(k, [0, 0]); g[0] += 1; g[1] += 1 if win else 0


KEY_LEN = 12  # длина ключа матча (первые символы sha1)


def read_keys(v):
    """Ключи матчей: в файле — одной строкой подряд (так короче), в старых файлах — списком."""
    if isinstance(v, str):
        return {v[i:i + KEY_LEN] for i in range(0, len(v), KEY_LEN)}
    return set(v or [])


def write_keys(keys):
    return "".join(sorted(keys))


def add_synergy(d, w, l, trios=True):
    """Связки: пары (и тройки) бойцов в одной команде."""
    for team, won in ((w, True), (l, False)):
        t = sorted(team)
        for i in range(len(t)):
            for j in range(i + 1, len(t)):
                add(d.setdefault("duo", {}), t[i] + "|" + t[j], won)
        if trios and len(t) == 3:
            add(d.setdefault("trio", {}), "|".join(t), won)


def merge_battles(state, battles, today, days_dir=DAYS_DIR, min_rank=MIN_RANK, full=True):
    """Добавляет новые матчи в дневные сводки. Повторы отсеиваются по ключу матча.
    full=False — облегчённые сводки (для всех рангов): только пары без троек. Они нужны лишь для
    оценки личных пиков, а тройки занимали много места."""
    days, fresh = {}, 0
    oldest = (today - dt.timedelta(days=WINDOW_DAYS - 1)).isoformat()
    ddir = os.path.join(state, days_dir)

    def open_day(day):
        d = load(os.path.join(ddir, day + ".json"), {"keys": "", "maps": {}, "vs": {}, "rk": {}})
        d["_k"] = read_keys(d.get("keys"))
        # «nosyn» — матчи, учтённые до появления связок: связки по ним досчитываются, если матч встретится снова.
        # (В старых файлах вместо этого был полный список «skeys» — переводим.)
        d["_n"] = (d["_k"] - read_keys(d.pop("skeys"))) if "skeys" in d else read_keys(d.get("nosyn"))
        if not full:
            d.pop("trio", None); d["_n"] = set()
        return d

    # все дни окна открываем сразу: так старые файлы переводятся в компактный формат, даже если новых матчей в них нет
    for fn in sorted(os.listdir(ddir)) if os.path.isdir(ddir) else []:
        if fn.endswith(".json") and oldest <= fn[:10] <= today.isoformat():
            days[fn[:10]] = open_day(fn[:10])
    for day, key, mode, mp, w, l, _tags, rank, (rw, rl) in battles:
        if min_rank and (rank is None or rank < min_rank):
            continue  # матч ниже нужного звания — не учитываем
        if day < oldest or day > today.isoformat():
            continue
        if day not in days:
            days[day] = open_day(day)
        d = days[day]
        if key in d["_k"]:
            if key in d["_n"]:
                d["_n"].discard(key)
                add_synergy(d, w, l)
            continue
        d["_k"].add(key); fresh += 1
        add_synergy(d, w, l, trios=full)
        m = d["maps"].setdefault(mode + "|" + mp, {})
        for team, won in ((w, True), (l, False)):
            for b in team:
                add(m, b, won)
        for a in w:
            for b in l:
                add(d["vs"].setdefault(a, {}), b, True)
                add(d["vs"].setdefault(b, {}), a, False)
        # средний ранг игроков на каждом бравлере: кто его берёт
        for team, ranks in ((w, rw), (l, rl)):
            for b, r in zip(team, ranks):
                if r:
                    x = d.setdefault("rk", {}).setdefault(b, [0, 0]); x[0] += r; x[1] += 1
    for day, d in days.items():
        d["keys"] = write_keys(d.pop("_k"))
        n = d.pop("_n")
        if n:
            d["nosyn"] = write_keys(n)
        else:
            d.pop("nosyn", None)
        save(os.path.join(state, days_dir, day + ".json"), d)
    # старые дни удаляем
    for fn in os.listdir(ddir) if os.path.isdir(ddir) else []:
        if fn[:10] < oldest:
            os.remove(os.path.join(ddir, fn))
    return fresh


# ---------- русские названия из русской вики ----------
def ru_name(en):
    q = urllib.parse.urlencode({"action": "query", "list": "search", "srsearch": f'"англ. {en}"',
                                "srlimit": "3", "format": "json"})
    d = http_json(RU_WIKI + "?" + q)
    for hit in (d or {}).get("query", {}).get("search", []):
        q2 = urllib.parse.urlencode({"action": "query", "titles": hit["title"], "prop": "revisions",
                                     "rvprop": "content", "rvslots": "main", "format": "json"})
        p = list(((http_json(RU_WIKI + "?" + q2) or {}).get("query", {}).get("pages", {}) or {"x": {}}).values())[0]
        txt = (p.get("revisions") or [{}])[0].get("slots", {}).get("main", {}).get("*", "")
        for m in re.finditer(r"англ\.?\s*(?:'')?(.+?)(?:'')?\s*[),;\]\n]", txt[:3000]):
            if norm(m.group(1)) == norm(en):
                return hit["title"]
    return None


def names_cache(state, wanted):
    cache = load(os.path.join(state, "names.json"), {})
    for en in sorted(wanted):
        if not cache.get(en):  # ненайденные пробуем снова в следующий раз
            try:
                cache[en] = ru_name(en)
            except Exception:
                continue
    save(os.path.join(state, "names.json"), cache)
    return cache


# ---------- итоговая сводка ----------
def adj(g, w, prior=50):
    return (w + prior * .5) / (g + prior)


_BRAWLER_INFO = {}


def brawler_info():
    """Справочник бравлеров (правильное написание и класс) — один запрос на весь запуск."""
    if "v" not in _BRAWLER_INFO:
        info = {}
        bl = http_json(BRAWLAPI + "/brawlers")
        for x in (bl or {}).get("list", []):
            info[norm(x["name"])] = {"name": x["name"], "cls": CLASS.get((x.get("class") or {}).get("name"), "")}
        _BRAWLER_INFO["v"] = info
    return _BRAWLER_INFO["v"]


def build_meta(state, today, players, days_dir=DAYS_DIR, min_rank=MIN_RANK, lookup_names=True):
    oldest_pool = (today - dt.timedelta(days=POOL_DAYS - 1)).isoformat()
    maps, vs, pool, rks, duo, trio = {}, {}, {}, {}, {}, {}
    total, covered, syn_total = 0, 0, 0
    ddir = os.path.join(state, days_dir)
    for fn in sorted(os.listdir(ddir)) if os.path.isdir(ddir) else []:
        d = load(os.path.join(ddir, fn), {})
        nk = len(read_keys(d.get("keys")))
        total += nk
        covered += 1 if nk else 0
        # с учётом связок: все матчи, кроме тех, что учтены до появления связок (в старых файлах — skeys)
        syn_total += len(read_keys(d["skeys"])) if "skeys" in d else nk - len(read_keys(d.get("nosyn")))
        for mk, bs in d.get("maps", {}).items():
            agg = maps.setdefault(mk, {})
            for b, (g, w) in bs.items():
                a = agg.setdefault(b, [0, 0]); a[0] += g; a[1] += w
            if fn[:10] >= oldest_pool:
                pool[mk] = pool.get(mk, 0) + sum(g for g, _ in bs.values()) // 6
        for src, dst in ((d.get("duo", {}), duo), (d.get("trio", {}), trio)):
            for k, (g, w) in src.items():
                x = dst.setdefault(k, [0, 0]); x[0] += g; x[1] += w
        for b, (sr, n) in d.get("rk", {}).items():
            x = rks.setdefault(b, [0, 0]); x[0] += sr; x[1] += n
        for a, row in d.get("vs", {}).items():
            r = vs.setdefault(a, {})
            for b, (g, w) in row.items():
                x = r.setdefault(b, [0, 0]); x[0] += g; x[1] += w

    overall, by_mode = {}, {}
    for mk, bs in maps.items():
        mode = mk.split("|")[0]
        for b, (g, w) in bs.items():
            for dct in (overall, by_mode.setdefault(mode, {})):
                a = dct.setdefault(b, [0, 0]); a[0] += g; a[1] += w

    # справочник бравлеров: правильное написание и класс
    info = brawler_info()
    def pretty(n):
        return info.get(norm(n), {}).get("name") or n.title()

    # Тир сайт считает сам — смешивает встроенный с винрейтом и популярностью (mergeBrawlers в index.html).
    min_games = 150  # меньше — винрейт слишком шумный: сайт не добавляет новых бойцов и не берёт их режимы

    # Везде ниже считаем «прирост»: насколько бравлер выигрывает в этой ситуации чаще, чем обычно.
    # Иначе просто сильный бравлер (например, с 60% побед) оказался бы «контрпиком» ко всем подряд.
    base = {b: adj(g, w) for b, (g, w) in overall.items()}

    modes = {}
    for mode, bs in by_mode.items():
        cand = [(adj(g, w) - base[b], b) for b, (g, w) in bs.items()
                if g >= 100 and adj(g, w) >= .5 and adj(g, w) - base[b] >= .015]
        for _, b in sorted(cand, reverse=True)[:12]:
            modes[b] = modes.get(b, "") + mode

    counters = {}
    for x in overall:
        cand = []
        for y, row in vs.items():
            g, w = row.get(x, [0, 0])
            if y != x and g >= 80 and adj(g, w, 100) >= .5 and adj(g, w, 100) - base.get(y, .5) >= .025:
                cand.append((adj(g, w, 100) - base.get(y, .5), y))
        counters[x] = [pretty(y) for _, y in sorted(cand, reverse=True)[:5]]

    map_list = []
    for mk, cnt in sorted(pool.items(), key=lambda kv: -kv[1]):
        if cnt < 15:
            continue
        mode, mp = mk.split("|", 1)
        bs = maps.get(mk, {})
        cand = [(adj(g, w, 100) - base[b], b) for b, (g, w) in bs.items()
                if g >= 60 and adj(g, w, 100) >= .51 and adj(g, w, 100) - base[b] >= .02]
        map_list.append({"name": mp, "mode": mode, "games": sum(g for g, _ in bs.values()) // 6,
                         "best": [pretty(b) for _, b in sorted(cand, reverse=True)[:6]]})

    # русские названия ищем в вики только для основной сводки; остальные берут уже найденные
    names = names_cache(state, {pretty(b) for b in overall} | {m["name"] for m in map_list}) if lookup_names \
        else load(os.path.join(state, "names.json"), {})
    for m in map_list:
        m["ru"] = names.get(m["name"])

    brawlers = {}
    for b, (g, w) in overall.items():
        p = pretty(b)
        brawlers[norm(b)] = {"name": p, "ru": names.get(p), "cls": info.get(norm(b), {}).get("cls", ""),
                             "games": g, "wr": round(w / g, 4),
                             "modes": "".join(sorted(set(modes.get(b, "")), key="GBHYKZ".index)),
                             "by": counters.get(b, []),
                             "avgRank": round(rks[b][0] / rks[b][1], 2) if rks.get(b, [0, 0])[1] else None,
                             # результаты против каждого соперника: [процент побед, матчей] — для советов под вражеский пик
                             "vs": {norm(y): [round(100 * wy / gy, 1), gy] for y, (gy, wy) in sorted(vs.get(b, {}).items())
                                    if gy >= 40}}
    # Связки: с кем боец выигрывает в одной команде. [процент побед вместе, матчей]
    for k, (g, w) in duo.items():
        a, b = k.split("|")
        if g >= 60 and norm(a) in brawlers and norm(b) in brawlers:
            brawlers[norm(a)].setdefault("with", {})[norm(b)] = [round(100 * w / g, 1), g]
            brawlers[norm(b)].setdefault("with", {})[norm(a)] = [round(100 * w / g, 1), g]
    trios = sorted(([*[norm(x) for x in k.split("|")], g, round(100 * w / g, 1)] for k, (g, w) in trio.items()
                    if g >= 30 and len(k.split("|")) == 3), key=lambda t: -t[3])[:1500]
    rall = [sum(x) for x in zip(*rks.values())] if rks else [0, 0]
    avg_all = rall[0] / rall[1] if rall[1] else None
    for k, an in analyze(overall, vs, maps, by_mode, rks, total, avg_all, pretty).items():
        brawlers[k]["an"] = an
    return {"updated": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%MZ"),
            "avgRank": round(rall[0] / rall[1], 2) if rall[1] else None,
            "windowDays": WINDOW_DAYS, "daysCovered": covered,
            "minRank": min_rank, "rankLabel": rank_name(min_rank) + " и выше" if min_rank else "все ранги", "battles": total, "players": players, "minGames": min_games,
            "brawlers": brawlers, "maps": map_list, "trios": trios, "synBattles": syn_total}


# ---------- разбор отдельных игроков (теги из players.txt) ----------
PLAYER_KEEP = 800  # сколько последних боёв хранить на игрока (ранкед и обычные)


def norm_tag(t):
    return re.sub(r"[^0-9A-Z]", "", t.upper().replace("O", "0"))


def read_tags(path):
    """Теги по одному в строке, с # или без. Комментарии — после «//» или строки, начинающиеся с «# » (решётка и пробел)."""
    tags = []
    try:
        lines = open(path, encoding="utf-8").read().splitlines()
    except FileNotFoundError:
        return tags
    for line in lines:
        line = line.split("//", 1)[0].strip()
        if not line or line.startswith("# "):
            continue
        t = norm_tag(line.split()[0])
        if 3 <= len(t) <= 14 and t not in tags:
            tags.append(t)
    return tags


PICK_SHRINK = 400  # сглаживание поправок пар: при 400 матчах пары берётся половина её отклонения
PICK_CAP = 6       # и не больше ±6% на одну пару — иначе случайный шум складывается и раздувает оценку


def pick_estimate(B, p, allies, enemies):
    """Оценка шанса на победу (в %) бойцом p при этих союзниках и врагах — по статистике всех рангов:
    общая сила + особенности пар против врагов + связки с союзниками (формулы как на сайте).
    Поправки сглажены и ограничены: оценка сравнивает ~100 вариантов, и без этого лучший из них
    почти всегда оказывался бы завышен просто из-за случайного разброса."""
    x = B.get(norm(p))
    if not x or x.get("wr") is None:
        return None

    def term(v, expected):
        return max(-PICK_CAP, min(PICK_CAP, (v[0] - expected) * v[1] / (v[1] + PICK_SHRINK)))

    s = 100 * x["wr"]
    for e in enemies:
        y, v = B.get(norm(e)), (x.get("vs") or {}).get(norm(e))
        if y and v:
            s += term(v, 100 * x["wr"] - 100 * y["wr"] + 50)
    for a in allies:
        y, v = B.get(norm(a)), (x.get("with") or {}).get(norm(a))
        if y and v:
            s += term(v, 100 * x["wr"] + 100 * y["wr"] - 50)
    return s


def rate_picks(battles, B):
    """Оценка каждого пика в ранкеде: b["an"] = [оценка, отрыв лучшей замены, три лучшие замены,
    доля вариантов, оценённых выше вашего пика (в %)]. Вердикт на сайте — по этой доле: она устойчива
    к тому, что максимум из ~100 оценок всегда немного завышен.
    Сводки (бойцы, соперники, режимы, «стоит попробовать») сайт считает сам из списка боёв."""
    pool = [k for k, x in B.items() if x.get("games", 0) >= 300]
    for b in battles:
        # Пик оцениваем только в ранкеде: в обычных боях пикают вслепую, врагов не видно.
        own = pick_estimate(B, b["b"], b["al"], b["en"]) if b.get("q", "r") == "r" else None
        alts = []
        if own is not None:
            used = {norm(n) for n in [b["b"], *b["al"], *b["en"]]}
            alts = sorted(((e, B[k]["name"]) for k in pool if k not in used
                           for e in [pick_estimate(B, B[k]["name"], b["al"], b["en"])] if e is not None),
                          key=lambda t: -t[0])
        if own is not None and alts:
            better = round(100 * sum(1 for e, _ in alts if e > own) / len(alts))
            b["an"] = [round(own, 1), round(alts[0][0] - own, 1), [[n, round(e, 1)] for e, n in alts[:3]], better]
        else:
            b.pop("an", None)


def track_players(key, state, tags, B):
    pdir = os.path.join(state, "players")
    os.makedirs(pdir, exist_ok=True)
    for t in tags:
        prof = api(player_path(t), key)
        if not prof:
            print(f"Игрок #{t}: профиль не найден — проверьте тег в players.txt")
            continue
        path = os.path.join(pdir, t + ".json")
        data = load(path, {"tag": "#" + t, "battles": []})
        known = {b["t"] for b in data["battles"]}
        new = 0
        log = api(player_path(t, "/battlelog"), key)
        if log is None:
            print(f"Игрок #{t}: история боёв недоступна — после техобслуживания она появляется с первым новым боем")
        for it in (log or {}).get("items", []):
            bt, ev = it.get("battle", {}), it.get("event") or {}
            teams = bt.get("teams") or []
            mode = MODES.get(ev.get("mode") or bt.get("mode"))
            # «ranked» в API — обычные (трофейные) бои; ранкед — soloRanked и teamRanked (см. is_ranked)
            ranked = is_ranked(it)
            if not (ranked or str(bt.get("type")).lower() == "ranked") or not mode or len(teams) != 2 \
                    or any(len(tm) != 3 for tm in teams) or len(it.get("battleTime") or "") < 15 \
                    or any(not (p.get("brawler") or {}).get("name") for tm in teams for p in tm) \
                    or bt.get("result") not in ("victory", "defeat") or it.get("battleTime") in known:
                continue  # берём все бои 3 на 3 в основных режимах: ранкед и обычные (трофейные)
            mine = next((i for i, tm in enumerate(teams) if any(norm_tag(p.get("tag", "")) == t for p in tm)), None)
            if mine is None:
                continue
            me = next(p for p in teams[mine] if norm_tag(p.get("tag", "")) == t)
            # в ранкеде brawler.trophies — номер ранга, в обычных боях — кубки бойца
            tr = (me.get("brawler") or {}).get("trophies")
            data["battles"].append({
                "t": it["battleTime"], "q": "r" if ranked else "t", "mode": mode, "map": ev.get("map"),
                "win": bt["result"] == "victory", "b": me["brawler"]["name"],
                "r": valid_rank(tr) if ranked else None, "bt": None if ranked else tr,
                "al": [p["brawler"]["name"] for p in teams[mine] if p is not me],
                "en": [p["brawler"]["name"] for p in teams[1 - mine]]})
            new += 1
        data["battles"] = sorted(data["battles"], key=lambda b: b["t"])[-PLAYER_KEEP:]
        data.update({"name": prof.get("name"), "trophies": prof.get("trophies"),
                     "updated": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%MZ")})
        data.pop("summary", None)  # сводку сайт считает сам из списка боёв
        rate_picks(data["battles"], B)
        # средний винрейт каждого из его бойцов по всем рангам — для сравнения на сайте
        # (как и для тиров: меньше 150 матчей — винрейт слишком шумный, сравнение не показываем)
        # русские названия карт его боёв (в обычных режимах бывают карты не из пула ранкеда)
        try:
            names = names_cache(state, {b["map"] for b in data["battles"] if b.get("map")})
            data["mapsRu"] = {m: names[m] for m in {b["map"] for b in data["battles"] if b.get("map")} if names.get(m)}
        except Exception:
            pass
        data["gwr"] = {}
        for n in {b["b"] for b in data["battles"]}:
            x = B.get(norm(n)) or {}
            if x.get("wr") is not None and x.get("games", 0) >= 150:
                data["gwr"][n] = round(x["wr"], 4)
        save(path, data)
        print(f"Игрок #{t} ({prof.get('name')}): новых матчей {new}, всего в истории {len(data['battles'])}")


def debug_player(key, tag):
    """Печатает номера рангов в матчах ранкеда одного игрока — чтобы сверить нумерацию со званием в игре.
    С тегом TOP печатает пятёрку лидеров мирового рейтинга."""
    if tag.strip().upper() in ("TOP", "ТОП"):
        for p in ((api("/rankings/global/players?limit=5", key) or {}).get("items") or []):
            print(f"  {p.get('rank')}. {p.get('tag')}  {p.get('name')}  {p.get('trophies')} кубков")
        return
    t = norm_tag(tag)
    prof = api(player_path(t), key)
    print(f"Профиль #{t}:", "не найден" if not prof else f"найден, кубков {prof.get('trophies')}")
    d = api(player_path(t, "/battlelog"), key)
    if not d:
        print(f"История боёв игрока #{t} недоступна (после техобслуживания она появляется, когда игрок сыграет новый бой).")
        return
    n = 0
    for it in d.get("items", []):
        bt = it.get("battle", {})
        if not is_ranked(it):
            continue
        n += 1
        own, others = None, []
        for team in bt.get("teams") or []:
            for p in team:
                r = (p.get("brawler") or {}).get("trophies")
                if norm_tag(p.get("tag", "")) == t:
                    own = r
                else:
                    others.append(r)
        print(f"  {it.get('battleTime', '')[:13]}  {(it.get('event') or {}).get('mode')}  {bt.get('result')}:"
              f"  номер ранга игрока = {own} ({rank_name(own) if isinstance(own, int) else '?'}), у остальных = {others}")
    print(f"Матчей ранкеда в истории #{t}: {n}")


def percentile(vals, q):
    vals = sorted(vals)
    return vals[min(len(vals) - 1, int(q * len(vals)))] if vals else 0


def analyze(overall, vs, maps, by_mode, rks, total, avg_all, pretty, min_games=300):
    """Разбор каждого бойца: кого бьёт и кого боится (с учётом силы обоих), где силён, кто на нём играет,
    и признаки «аномальности». Возвращает {ключ: разбор} для бойцов, у кого хватает матчей."""
    slots = sum(g for g, _ in overall.values()) or 1
    freq = {b: g / slots for b, (g, _) in overall.items()}
    wr = {b: w / g for b, (g, w) in overall.items() if g}

    def shrink(x, g, k=150):
        return x * g / (g + k)

    res = {}
    for b, (g, w) in overall.items():
        if g < min_games:
            continue
        x = wr[b]
        inter = []
        for y, (gy, wy) in vs.get(b, {}).items():
            if gy >= 60 and y in wr:
                inter.append((shrink(100 * wy / gy - (100 * x - 100 * wr[y] + 50), gy), y, 100 * wy / gy, gy))
        inter.sort(reverse=True)
        sit = (sum(gy * i * i for i, _, _, gy in inter) / sum(gy for *_, gy in inter)) ** .5 if inter else 0
        num = den = 0
        for y, (gy, wy) in vs.get(b, {}).items():
            if y in freq:
                num += freq[y] * (wy + 20 * x) / (gy + 20); den += freq[y]
        wr_adj = num / den if den else x
        mp = []
        for mk, bs in maps.items():
            gm, wm = bs.get(b, [0, 0])
            if gm >= 60:
                mode, name = mk.split("|", 1)
                mp.append((shrink(100 * (wm / gm - x), gm), name, mode, 100 * wm / gm, gm))
        mp.sort(reverse=True)
        map_sd = (sum(l * l for l, *_ in mp) / len(mp)) ** .5 if mp else 0
        md = []
        for mode, bs in by_mode.items():
            gm, wm = bs.get(b, [0, 0])
            if gm >= 100:
                md.append((shrink(100 * (wm / gm - x), gm), mode, 100 * wm / gm, gm))
        md.sort(reverse=True)
        rn = rks.get(b, [0, 0])
        res[norm(b)] = {
            "pick": round(100 * g / total, 1) if total else 0,
            "wrAdj": round(wr_adj, 4),
            "sit": round(sit, 2), "mapSd": round(map_sd, 2),
            "rankN": rn[1], "rankDelta": round(rn[0] / rn[1] - avg_all, 2) if rn[1] and avg_all else None,
            "good": [[pretty(y), round(v, 1), gy, round(i, 1)] for i, y, v, gy in inter if i >= 3][:6],
            "bad": [[pretty(y), round(v, 1), gy, round(i, 1)] for i, y, v, gy in reversed(inter) if i <= -3][:6],
            "mapsGood": [[n, m, round(v, 1), gm, round(l, 1)] for l, n, m, v, gm in mp if l >= 2][:4],
            "mapsBad": [[n, m, round(v, 1), gm, round(l, 1)] for l, n, m, v, gm in reversed(mp) if l <= -2][:4],
            "modesGood": [[m, round(v, 1), gm, round(l, 1)] for l, m, v, gm in md if l >= 1.5][:3],
            "modesBad": [[m, round(v, 1), gm, round(l, 1)] for l, m, v, gm in reversed(md) if l <= -1.5][:3],
        }
    if not res:
        return res
    # Признаки аномальности — относительно остальных бойцов
    keys = {norm(b): b for b in overall}
    picks = [a["pick"] for a in res.values()]
    sits = [a["sit"] for a in res.values()]
    msds = [a["mapSd"] for a in res.values()]
    rdel = [a["rankDelta"] for a in res.values() if a["rankDelta"] is not None and a["rankN"] >= 300]
    p_med, p_hi = percentile(picks, .5), percentile(picks, .85)
    s_hi, m_hi = percentile(sits, .85), percentile(msds, .85)
    r_hi = max(.2, percentile(rdel, .9)) if rdel else None
    for k, a in res.items():
        x = wr[keys[k]]
        f = []
        if x >= .56: f.append("high")
        if x <= .45: f.append("low")
        if x >= .54 and a["pick"] <= p_med: f.append("hidden")
        if a["pick"] >= p_hi and x <= .48: f.append("overrated")
        if a["sit"] >= s_hi and a["sit"] >= 3: f.append("situational")
        if a["mapSd"] >= m_hi and a["mapSd"] >= 3: f.append("mapdep")
        if r_hi is not None and a["rankN"] >= 300 and a["rankDelta"] is not None and a["rankDelta"] >= r_hi: f.append("skill")
        if x - a["wrAdj"] >= .015: f.append("easyopp")
        a["flags"] = f
    return res


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--state", default="stats")
    ap.add_argument("--max-players", type=int, default=0)
    ap.add_argument("--players", default="players.txt", help="файл с тегами игроков для личного разбора")
    a = ap.parse_args()
    key = os.environ.get("BS_API_KEY", "").strip()
    if not key:
        raise SystemExit("Нет ключа BS_API_KEY. Как его получить — в README, раздел «Автообновление».")
    if os.environ.get("BS_DEBUG_TAG"):
        try:
            debug_player(key, os.environ["BS_DEBUG_TAG"])
        except Maintenance:
            print("::warning::API Brawl Stars на техобслуживании — проверить игрока сейчас нельзя.")
        raise SystemExit(0)
    today = dt.datetime.now(dt.timezone.utc).date()
    try:
        tags = collect_players(key)
    except Maintenance:
        print("::warning::API Brawl Stars на техобслуживании — оставляю вчерашние данные, попробую завтра.")
        return
    limit = a.max_players or env_num("BS_MAX_PLAYERS", 0)
    if limit:
        tags = tags[:limit]
    if not tags:
        raise SystemExit("API не вернул ни одного игрока — проверьте ключ.")
    if not pick_tag_format(key, tags):
        print("::warning::API не отдаёт истории боёв ни в одном формате тега — данные не обновлены.")
        return
    max_logs = env_num("BS_MAX_LOGS", 20000)
    deadline = time.time() + 60 * env_num("BS_TIME_BUDGET", 20, float)
    if limit:
        max_logs = min(max_logs, limit * 3)
    try:
        battles, _ = crawl(key, tags, max_logs, deadline)
        polled = DIAG["logs_ok"]
        print_diag()
    except Maintenance:
        print("::warning::API Brawl Stars ушёл на техобслуживание во время сбора — оставляю прошлые данные.")
        return
    shutil.rmtree(os.path.join(a.state, "days"), ignore_errors=True)  # старые сводки без учёта звания
    fresh = merge_battles(a.state, battles, today)
    merge_battles(a.state, battles, today, DAYS_ALL, 0, full=False)
    meta = build_meta(a.state, today, polled)
    save(os.path.join(a.state, "meta.json"), meta)
    tracked = read_tags(a.players)
    # истории игроков, которых убрали из списка, удаляем (только если сам список на месте)
    pdir = os.path.join(a.state, "players")
    if os.path.isfile(a.players) and os.path.isdir(pdir):
        for fn in os.listdir(pdir):
            if fn.endswith(".json") and fn[:-5] not in tracked:
                os.remove(os.path.join(pdir, fn))
                print(f"Игрок #{fn[:-5]} больше не в players.txt — его история удалена.")
    if tracked:
        # сводка по всем рангам нужна только для личного разбора (сами дни копятся всегда — см. выше)
        meta_all = build_meta(a.state, today, polled, DAYS_ALL, 0, lookup_names=False)
        save(os.path.join(a.state, "meta_all.json"), meta_all)
        print(f"Все ранги: {meta_all['battles']} матчей ранкеда за {WINDOW_DAYS} дн.")
        try:
            track_players(key, a.state, tracked, meta_all["brawlers"])
        except Maintenance:
            print("::warning::API ушёл на техобслуживание при разборе игроков.")
    print(f"Учитываются матчи со средним рангом от {rank_name(MIN_RANK)} (номер {MIN_RANK}).")
    print(f"Игроков опрошено: {polled}, матчей ранкеда в логах: {len(battles)}, новых: {fresh}, "
          f"всего за {WINDOW_DAYS} дн.: {meta['battles']}, карт в пуле: {len(meta['maps'])}")
    if meta["battles"] == 0:
        print("::warning::Матчей ранкеда не найдено. Возможно, в API изменился тип боя.")


if __name__ == "__main__":
    main()
