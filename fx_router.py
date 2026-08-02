#!/usr/bin/env python3
"""
fx_router — поиск оптимальной цепочки P2P-конвертации валют через Bybit.

Ищет путь RUB → (USD|KZT) с максимальным эффективным курсом, перебирая
все маршруты через крипто-мосты (USDT, USDC, BTC, ETH) и другие фиатные валюты.

CLI:
    python -m fx_router --amount 100000 --target USD --max-hops 3
"""

from __future__ import annotations

import argparse
import concurrent.futures
import hmac
import json
import logging
import math
import os
import statistics
import sys
import time
from dataclasses import dataclass, field
from typing import Any

import httpx

log = logging.getLogger("fx_router")

# ──────────────────────────────────────────────────────────────────────
# Конфиг
# ──────────────────────────────────────────────────────────────────────

FIAT_CURRENCIES: tuple[str, ...] = (
    "RUB",  # старт
    "KZT", "USD", "EUR",          # целевые (Freedom Bank)
    "AED",                         # дирхам ОАЭ, пег к USD, активный P2P
    "TRY",                         # лира, самый ликвидный P2P после RUB
    "UZS", "KGS", "AMD", "GEL",    # Средняя Азия / Кавказ
    "AZN", "TJS",                  # Азербайджан, Таджикистан
    "CNY", "HKD",                  # юань, гонконгский доллар
    "INR", "VND", "THB", "IDR",    # Азия с активным P2P
    "BRL", "COP", "ARS",           # ЛатАм, широкие спреды = аномалии
    "NGN", "EGP",                  # Африка, то же
    "GBP", "PLN", "RON", "UAH",    # Европа
)

CRYPTO_CURRENCIES: tuple[str, ...] = (
    "USDT",  # основной мост, максимальная ликвидность P2P
    "USDC",  # второй стейбл, иногда лучше курс
    "BTC",
    "ETH",
)


@dataclass
class Config:
    """Настройки fx_router. Все поля имеют дефолты — модуль работает из коробки."""

    # Доступные валюты (вершины графа)
    fiat_currencies: tuple[str, ...] = field(default_factory=lambda: FIAT_CURRENCIES)
    crypto_currencies: tuple[str, ...] = field(default_factory=lambda: CRYPTO_CURRENCIES)

    # Источник данных
    bybit_p2p_url: str = "https://api2.bybit.com/fiat/otc/item/online"
    bybit_p2p_page_size: int = 50           # объявлений на страницу
    bybit_p2p_pages: int = 0                # 0 = ВЕСЬ стакан (все страницы), N = только первые N

    # Фильтры мейкеров
    min_recent_order_num: int = 100
    min_recent_execute_rate: float = 99.0
    antiscam_threshold: float = 0.10  # отклонение от медианы всех валидных (±10%): режет старьё (-15%+), пускает реальные лучшие цены
    max_ad_age_hours: float = 336.0   # объявления старше 14 дней — мусор (цены неактуальны)
    blacklist_keywords: tuple[str, ...] = ("pyypl", "доверенн")

    # Фильтры способов оплаты (по ID или подстроке названия, например "Freedom Bank").
    # pay_in_methods — как юзер ПЛАТИТ за крипту (side=1, фиат→крипта, мейкеры продают).
    # pay_out_methods — как юзер ПОЛУЧАЕТ фиат (side=0, крипта→фиат, мейкеры покупают).
    pay_in_methods: tuple[str, ...] = ()
    pay_out_methods: tuple[str, ...] = ()

    # Конверсии Freedom Bank (bankffin.kz) как рёбра графа фиат→фиат
    use_bank_edges: bool = False
    bank_rates_section: str = "mobile"   # "mobile" — приложение, "cash" — отделение

    # Поиск путей
    max_hops: int = 3
    amount_rub: float = 100_000.0  # сумма сделки в RUB

    # HTTP
    http_timeout: float = 15.0

    # V5 API аутентификация (опционально). Если пусто — используется публичный api2 endpoint.
    bybit_api_key: str = ""
    bybit_api_secret: str = ""
    bybit_recv_window: str = "60000"


# ──────────────────────────────────────────────────────────────────────
# Модели
# ──────────────────────────────────────────────────────────────────────


@dataclass
class Maker:
    """Одно объявление мейкера на P2P-площадке."""

    ad_id: str = ""                    # ID объявления
    user_id: str = ""                  # ID пользователя (для ссылки)
    token_id: str = ""                 # криптовалюта (USDT, ...)
    currency_id: str = ""              # фиатная валюта (RUB, ...)
    price: float = 0.0                 # курс
    min_amount: float = 0.0            # мин. сумма сделки (в фиате объявления)
    max_amount: float = 0.0            # макс. сумма сделки
    recent_order_num: int = 0          # количество сделок
    recent_execute_rate: float = 0.0   # процент выполнения
    is_online: bool = False            # онлайн ли мейкер
    nickname: str = ""                 # никнейм
    remark: str = ""                   # условие объявления (для blacklist)
    create_date_ms: int = 0            # когда создано объявление (ms epoch, 0 = неизвестно)
    payments: list[str] = field(default_factory=list)  # ID способов оплаты мейкера

    @property
    def url(self) -> str:
        """Прямая ссылка на объявление в Bybit P2P."""
        if self.user_id and self.token_id and self.currency_id:
            return f"https://www.bybit.com/en/p2p/profile/{self.user_id}/{self.token_id}/{self.currency_id}/item"
        return ""


@dataclass
class Edge:
    """Ребро графа: конвертация из одной валюты в другую."""

    source: str          # валюта-источник (RUB, USDT, …)
    target: str          # целевая валюта
    rate: float          # эффективный курс (source → target)
    makers: list[Maker]  # мейкеры, по которым считали (пусто для банковских рёбер)
    side: str            # "1" — мейкеры продают крипту, "0" — мейкеры покупают, "bank" — конверсия банка
    is_bank: bool = False  # ребро — конверсия Freedom Bank по официальному курсу


@dataclass
class PathResult:
    """Результат поиска одного маршрута."""

    chain: list[str]       # ["RUB", "USDT", "USD"]
    edges: list[Edge]      # рёбра цепочки
    effective_rate: float  # RUB → итоговая валюта
    volume: float          # доступный объём (лимит мин. из всех шагов)
    score: float           # скор


# ──────────────────────────────────────────────────────────────────────
# Bybit P2P клиент
# ──────────────────────────────────────────────────────────────────────


def _v5_sign(secret: str, payload_str: str, timestamp: str, recv_window: str, api_key: str) -> str:
    """HMAC-SHA256 подпись для Bybit V5 API."""
    param_str = f"{timestamp}{api_key}{recv_window}{payload_str}"
    return hmac.new(secret.encode(), param_str.encode(), "sha256").hexdigest()


def _v5_prepare(payload: dict[str, Any], api_key: str, api_secret: str, recv_window: str) -> tuple[str, dict[str, str], str]:
    """Подготовить V5-запрос: сериализовать тело, подписать, вернуть (url, headers, body_str)."""
    body_str = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
    timestamp = str(int(time.time() * 1000))
    sign = _v5_sign(api_secret, body_str, timestamp, recv_window, api_key)
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "X-BAPI-API-KEY": api_key,
        "X-BAPI-SIGN": sign,
        "X-BAPI-TIMESTAMP": timestamp,
        "X-BAPI-RECV-WINDOW": recv_window,
    }
    return "https://api.bybit.com/v5/p2p/item/online", headers, body_str


def _fetch_page(
    url: str, payload: dict[str, Any], headers: dict[str, str],
    body_str: str | None, client: httpx.Client, timeout: float,
) -> tuple[list[dict[str, Any]], int]:
    """Выполнить один запрос и вернуть (items, total_count)."""
    resp = client.post(
        url,
        json=payload if body_str is None else None,
        content=body_str.encode() if body_str is not None else None,
        headers=headers,
        timeout=timeout,
    )
    try:
        data = resp.json()
    except Exception:
        return [], 0
    if data.get("ret_code") != 0 and data.get("retCode") != 0:
        return [], 0
    result = data.get("result", {})
    items = list(result.get("items") or result.get("data") or [])
    total = int(result.get("count", len(items)))
    return items, total


def fetch_ads(
    token_id: str,
    currency_id: str,
    side: str,  # "1" — мейкеры ПРОДАЮТ крипту (юзер покупает), "0" — мейкеры ПОКУПАЮТ (юзер продаёт)
    cfg: Config,
    client: httpx.Client,
) -> list[dict[str, Any]]:
    """Запросить P2P-объявления Bybit.

    По умолчанию тянет ВЕСЬ стакан (все страницы), чтобы не пропускать
    выгодные объявления, застрявшие глубоко в книге (сортировка api2.bybit.com
    нестабильна — лучшая цена может быть не на первой странице).
    Ограничить можно через cfg.bybit_p2p_pages или --pages N.
    Мусор (годами висящие объявления) отсекают фильтры свежести и анти-скам.
    """
    use_v5 = bool(cfg.bybit_api_key and cfg.bybit_api_secret)
    page_size = cfg.bybit_p2p_page_size

    def _make_payload(page: int) -> dict[str, Any]:
        return {"tokenId": token_id, "currencyId": currency_id, "side": side,
                "page": str(page), "size": str(page_size)}

    def _make_request(page: int) -> tuple[list[dict[str, Any]], int]:
        payload = _make_payload(page)
        if use_v5:
            url, headers, body_str = _v5_prepare(
                payload, cfg.bybit_api_key, cfg.bybit_api_secret, cfg.bybit_recv_window,
            )
        else:
            url = cfg.bybit_p2p_url
            headers = {
                "Content-Type": "application/json", "Accept": "application/json",
                "Origin": "https://www.bybit.com",
                "Referer": "https://www.bybit.com/en/fiat/trade/otc/",
                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/125.0.0.0 Safari/537.36",
            }
            body_str = None
        return _fetch_page(url, payload, headers, body_str, client, cfg.http_timeout)

    # Первый запрос: узнаём total_count
    all_items: list[dict[str, Any]] = []
    try:
        items, total = _make_request(1)
        all_items.extend(items)
    except Exception as exc:
        log.warning("HTTP error for %s/%s side=%s p1: %s", token_id, currency_id, side, exc)
        return []

    # Сколько всего страниц?
    total_pages = max(1, math.ceil(total / page_size)) if total > 0 else 1

    # Запрашиваем страницы: если cfg.bybit_p2p_pages == 0 — ВЕСЬ стакан,
    # иначе только первые N. Жёсткий потолок на пару — защита от аномалий.
    max_pages = 50
    if cfg.bybit_p2p_pages and cfg.bybit_p2p_pages > 0:
        pages_to_fetch = min(cfg.bybit_p2p_pages, total_pages, max_pages)
    else:
        pages_to_fetch = min(total_pages, max_pages)

    for p in range(2, pages_to_fetch + 1):
        try:
            items, _ = _make_request(p)
            all_items.extend(items)
        except Exception as exc:
            log.warning("HTTP error for %s/%s side=%s p%s: %s", token_id, currency_id, side, exc)
            continue

    log.info(
        "Fetched %d ads for %s→%s side=%s (%s, pages 1..%s of %s)",
        len(all_items), currency_id, token_id, side,
        "v5" if use_v5 else "public", pages_to_fetch, total_pages,
    )
    return all_items


def parse_makers(items: list[dict[str, Any]]) -> list[Maker]:
    """Преобразовать сырые объявления Bybit в список ``Maker``.

    Пытается читать поля в snake_case и camelCase.
    """
    makers: list[Maker] = []
    for item in items:
        try:
            price = float(item.get("price", 0) or 0)
            if price <= 0:
                continue

            min_am = _f(item, "minAmount", "min_amount")
            max_am = _f(item, "maxAmount", "max_amount")
            recent_num = int(item.get("recentOrderNum", item.get("recent_order_num", 0)) or 0)
            exec_rate = float(item.get("recentExecuteRate", item.get("recent_execute_rate", 0)) or 0)
            is_online = bool(item.get("isOnline", item.get("is_online", False)))
            nickname = str(item.get("nickName", item.get("nickname", "") or ""))
            remark = str(item.get("remark", "") or "")
            token = str(item.get("tokenId", ""))
            currency = str(item.get("currencyId", ""))
            payments = [str(p) for p in (item.get("payments") or [])]

            maker = Maker(
                ad_id=str(item.get("id", item.get("itemId", "")) or ""),
                user_id=str(item.get("userMaskId", item.get("userId", "")) or ""),
                token_id=token,
                currency_id=currency,
                price=price,
                min_amount=min_am,
                max_amount=max_am,
                recent_order_num=recent_num,
                recent_execute_rate=exec_rate,
                is_online=is_online,
                nickname=nickname,
                remark=remark,
                create_date_ms=_i64(item, "createDate"),
                payments=payments,
            )
            makers.append(maker)
        except (ValueError, TypeError):
            continue
    return makers


def _f(d: dict, *keys: str) -> float:
    """Достать float из dict по первому существующему ключу."""
    for k in keys:
        v = d.get(k)
        if v is not None:
            try:
                return float(v)
            except (ValueError, TypeError):
                pass
    return 0.0


def _i64(d: dict, key: str) -> int:
    """Достать int из dict (значение может быть str или число)."""
    v = d.get(key)
    if v is None:
        return 0
    try:
        return int(float(v))
    except (ValueError, TypeError):
        return 0


# ──────────────────────────────────────────────────────────────────────
# Справочник способов оплаты Bybit (id → название)
# ──────────────────────────────────────────────────────────────────────

_PAYMENT_DICT_URL = "https://api2.bybit.com/fiat/otc/configuration/queryAllPaymentList"
_payment_dict: dict[str, str] | None = None


def load_payment_dict(client: httpx.Client | None = None) -> dict[str, str]:
    """Загрузить справочник способов оплаты {paymentType: paymentName}.

    Результат кэшируется в модульной переменной. При ошибке возвращает {}.
    """
    global _payment_dict
    if _payment_dict is not None:
        return _payment_dict
    try:
        own = client is None
        client = client or httpx.Client()
        try:
            resp = client.post(_PAYMENT_DICT_URL, json={}, timeout=10.0)
            data = resp.json()
        finally:
            if own:
                client.close()
        d: dict[str, str] = {}
        for cfg in (data.get("result", {}).get("paymentConfigVo") or []):
            pid = cfg.get("paymentType")
            nm = cfg.get("paymentName")
            if pid and nm:
                d[str(pid)] = str(nm)
        _payment_dict = d
        log.info("Payment dict loaded: %d methods", len(d))
    except Exception as exc:
        log.warning("Failed to load payment dict: %s", exc)
        _payment_dict = {}
    return _payment_dict


def resolve_payment_ids(specs: tuple[str, ...], id2name: dict[str, str]) -> set[str]:
    """Преобразовать список способов оплаты в множество ID.

    Каждый spec может быть числовым ID ("549") или подстрокой названия
    ("freedom" → Freedom Bank). Регистр не важен.
    """
    ids: set[str] = set()
    for spec in specs:
        s = spec.strip()
        if not s:
            continue
        if s.isdigit():
            ids.add(s)
            continue
        matched = {pid for pid, name in id2name.items() if s.lower() in name.lower()}
        if matched:
            ids.update(matched)
            log.info("  payment '%s' → IDs %s (%s)", s, sorted(matched),
                     ", ".join(id2name.get(m, "?") for m in sorted(matched)))
        else:
            log.warning("  payment '%s' не найден в справочнике", s)
    return ids


# ──────────────────────────────────────────────────────────────────────
# Курсы конверсии Freedom Bank (bankffin.kz)
# ──────────────────────────────────────────────────────────────────────

_BANK_RATES_URL = "https://bankffin.kz/api/exchange-rates/getRates"


def fetch_bank_rates(section: str = "mobile") -> dict[tuple[str, str], float]:
    """Официальные курсы Freedom Bank → dict[(from, to)] = курс.

    Для пары (buyCode=A, sellCode=B, buyRate=b, sellRate=s):
      * A → B = b      — продаёшь A банку, получаешь B за единицу A
      * B → A = 1/s    — покупаешь A у банка, платишь B за единицу A

    ``section``: "mobile" — тариф мобильного приложения, "cash" — отделения.
    При ошибке возвращает {}.
    """
    try:
        with httpx.Client() as client:
            resp = client.get(_BANK_RATES_URL, timeout=10.0)
            data = resp.json().get("data", {})
        rates: dict[tuple[str, str], float] = {}
        for row in data.get(section) or []:
            a = str(row.get("buyCode", ""))
            b = str(row.get("sellCode", ""))
            if not a or not b or a == b:
                continue
            def _num(v: str) -> float | None:
                try:
                    return float(str(v).replace(" ", "").replace(",", "."))
                except ValueError:
                    return None
            buy = _num(row.get("buyRate", ""))
            sell = _num(row.get("sellRate", ""))
            if buy and buy > 0:
                rates[(a, b)] = buy
            if sell and sell > 0:
                rates[(b, a)] = 1.0 / sell
        log.info("Freedom Bank rates (%s): %d edges", section, len(rates))
        return rates
    except Exception as exc:
        log.warning("Failed to fetch Freedom Bank rates: %s", exc)
        return {}


# ──────────────────────────────────────────────────────────────────────
# Фильтры мейкеров
# ──────────────────────────────────────────────────────────────────────


def filter_makers(
    makers: list[Maker],
    side: str,         # "1" — покупаем крипту (мейкеры продают), "0" — продаём (мейкеры покупают)
    amount: float,     # сумма сделки в фиате объявления (0 = пропустить лимитную проверку)
    cfg: Config,
    required_payment_ids: set[str] | None = None,
    relaxed: bool = False,
) -> list[Maker]:
    """Отфильтровать мейкеров по критериям качества.

    Если ``amount`` == 0, проверка лимитов (minAmount/maxAmount) пропускается —
    это нужно при построении графа, когда реальный объём ещё неизвестен.

    ``relaxed=True`` — ослабленные пороги для тонких рынков (например, когда
    активен фильтр способа оплаты): не проверяется свежесть, достаточно
    30 сделок и 97% выполнения.

    * ``recentOrderNum >= min_recent_order_num`` (30 в relaxed)
    * ``recentExecuteRate >= min_recent_execute_rate`` (97% в relaxed)
    * мейкер онлайн
    * лимиты объявления вмещают сумму
    * условие не содержит blacklist-подстрок
    * объявление свежее ``max_ad_age_hours`` (пропускается в relaxed)
    * если ``required_payment_ids`` задан — мейкер поддерживает хотя бы один из них
    * анти-скам: курс не отклоняется от медианы ВСЕХ валидных больше чем на ``antiscam_threshold``
    """
    total = len(makers)
    now_ms = time.time() * 1000
    min_orders = 30 if relaxed else cfg.min_recent_order_num
    min_exec = 97.0 if relaxed else cfg.min_recent_execute_rate

    # Базовые фильтры
    valid: list[Maker] = []
    for m in makers:
        if not m.is_online:
            continue
        if m.recent_order_num < min_orders:
            continue
        if m.recent_execute_rate < min_exec:
            continue
        if amount and (amount < m.min_amount or amount > m.max_amount):
            continue
        # Свежесть: старые объявления висят годами с ценами из другой эпохи
        if not relaxed and m.create_date_ms > 0 and (now_ms - m.create_date_ms) > cfg.max_ad_age_hours * 3600_000:
            continue
        # Способ оплаты: мейкер должен поддерживать хотя бы один из требуемых
        if required_payment_ids and not (set(m.payments) & required_payment_ids):
            continue

        # Blacklist по условиям
        remark_lower = m.remark.lower()
        blocked = False
        for kw in cfg.blacklist_keywords:
            if kw.lower() in remark_lower:
                blocked = True
                break
        if blocked:
            continue

        valid.append(m)

    log.info("  base filters: %d → %d", total, len(valid))

    if len(valid) < 3:
        # Слишком мало мейкеров — дальше не фильтруем
        return valid

    # Анти-скам: отбрасываем аномальные курсы относительно медианы
    # ВСЕХ валидных объявлений (а не топ-10 самых дешёвых — на них
    # кучкуется старый скам-мусор по одной цене).
    prices = sorted(m.price for m in valid)
    median = statistics.median(prices)
    lower = median * (1.0 - cfg.antiscam_threshold)
    upper = median * (1.0 + cfg.antiscam_threshold)
    result = [m for m in valid if lower <= m.price <= upper]

    log.info("  anti-scam filter: %d → %d (median=%.4f, band %.4f..%.4f)",
             len(valid), len(result), median, lower, upper)
    return result


# ──────────────────────────────────────────────────────────────────────
# Построение графа
# ──────────────────────────────────────────────────────────────────────


def _edge_rate(makers: list[Maker], side: str) -> float:
    """Курс ребра = медиана топ-3 ЛУЧШИХ по цене.

    side "0" (крипта→фиат, юзер продаёт крипту): лучшие = самые ДОРОГИЕ (кто больше платит).
    side "1" (фиат→крипта, юзер покупает): лучшие = самые ДЕШЁВЫЕ.
    """
    if side == "0":
        top3 = sorted(makers, key=lambda m: m.price, reverse=True)[:3]
    else:
        top3 = sorted(makers, key=lambda m: m.price)[:3]
    return statistics.median(m.price for m in top3)


def build_graph(
    cfg: Config,
) -> dict[tuple[str, str], Edge]:
    """Построить ориентированный граф конвертаций P2P.

    Для каждой пары (фиат → крипта) и (крипта → фиат) запрашивает
    объявления, фильтрует и вычисляет курс ребра.
    Запросы выполняются параллельно (ThreadPool, 16 воркеров).

    Returns
    -------
    dict[(source, target), Edge]
        Рёбра со ставками.
    """
    all_currencies: list[str] = list(cfg.fiat_currencies) + list(cfg.crypto_currencies)
    edges: dict[tuple[str, str], Edge] = {}

    # Резолвим способы оплаты в ID (по названию или ID) один раз
    id2name = load_payment_dict()
    pay_in_ids = resolve_payment_ids(cfg.pay_in_methods, id2name) if cfg.pay_in_methods else set()
    pay_out_ids = resolve_payment_ids(cfg.pay_out_methods, id2name) if cfg.pay_out_methods else set()
    if pay_in_ids:
        log.info("Pay-in filter: %s", sorted(pay_in_ids))
    if pay_out_ids:
        log.info("Pay-out filter: %s", sorted(pay_out_ids))

    # Для каждой пары (source, target), где source != target
    # Ребро существует если source и target разной природы (fiat↔️crypto)
    # или обе одного типа — тоже можно, но нереалистично на P2P

    fiats = set(cfg.fiat_currencies)
    cryptos = set(cfg.crypto_currencies)

    pairs: list[tuple[str, str, str, str, str, float]] = []

    for src in all_currencies:
        for dst in all_currencies:
            if src == dst:
                continue

            if src in fiats and dst in cryptos:
                # Фиат→крипта (юзер ПОКУПАЕТ крипту): нужны объявления «Продам» = side "1"
                # (side "0" на api2 возвращает «Куплю» — там юзер может только продать крипту)
                amt = cfg.amount_rub if src == "RUB" else 0.0
                pairs.append((src, dst, src, dst, "1", amt))
            elif src in cryptos and dst in fiats:
                # Крипта→фиат (юзер ПРОДАЁТ крипту): нужны объявления «Куплю» = side "0"
                # сумма в крипте неизвестна до построения пути
                pairs.append((src, dst, dst, src, "0", 0.0))
            # else: skip fiat↔️fiat or crypto↔️crypto

    def _fetch_one(pair: tuple[str, str, str, str, str, float]) -> tuple[tuple[str, str], Edge] | None:
        """Запросить и отфильтровать одну пару. Вызывается в ThreadPool."""
        src, dst, fiat_currency, token_id, side, step_amount = pair
        # side "1" = покупаем крипту (платим фиат) → фильтр pay_in; side "0" = продаём → pay_out
        req_pay = pay_in_ids if side == "1" else pay_out_ids
        with httpx.Client() as client:
            raw = fetch_ads(token_id, fiat_currency, side, cfg, client)
        makers = parse_makers(raw)
        if not makers:
            return None
        valid = filter_makers(makers, side, step_amount, cfg, req_pay or None)
        if len(valid) < 3 and req_pay:
            # Тонкий рынок (фильтр оплаты): пробуем ослабленные пороги
            valid = filter_makers(makers, side, step_amount, cfg, req_pay or None, relaxed=True)
            if len(valid) >= 3:
                log.info("  relaxed filters for %s→%s (payment filter)", src, dst)
        if len(valid) < 3:
            return None
        raw_rate = _edge_rate(valid, side)
        # side "1" (фиат→крипта): цена объявления = фиат за крипту → курс = 1/цена
        # side "0" (крипта→фиат): цена = фиат за крипту → курс = цена
        if side == "1":
            rate = 1.0 / raw_rate
        else:
            rate = raw_rate
        return (src, dst), Edge(source=src, target=dst, rate=rate, makers=valid, side=side)

    max_workers = min(16, len(pairs) or 1)
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = [pool.submit(_fetch_one, p) for p in pairs]
        for f in concurrent.futures.as_completed(futures):
            try:
                result = f.result()
                if result is not None:
                    key, edge = result
                    edges[key] = edge
            except Exception as exc:
                log.warning("Error fetching pair: %s", exc)

    # Рёбра конверсий Freedom Bank (фиат→фиат по официальному курсу)
    if cfg.use_bank_edges:
        bank_rates = fetch_bank_rates(cfg.bank_rates_section)
        added = 0
        for (a, b), rate in bank_rates.items():
            if a in fiats and b in fiats and (a, b) not in edges:
                edges[(a, b)] = Edge(
                    source=a, target=b, rate=rate, makers=[], side="bank", is_bank=True,
                )
                added += 1
        log.info("Bank edges added: %d", added)

    log.info("Graph built: %d edges", len(edges))
    return edges


# ──────────────────────────────────────────────────────────────────────
# DFS поиск путей
# ──────────────────────────────────────────────────────────────────────


def find_paths(
    source: str,
    target: str,
    edges: dict[tuple[str, str], Edge],
    max_hops: int,
    cfg: Config,
) -> list[PathResult]:
    """DFS поиск всех путей ``source → target`` длиной до ``max_hops``.

    Строит ацикличные пути (без повторения валют).
    """
    # Построим adjacency list
    adj: dict[str, list[tuple[str, Edge]]] = {}
    for (u, v), edge in edges.items():
        if u not in adj:
            adj[u] = []
        adj[u].append((v, edge))

    results: list[PathResult] = []

    def _dfs(current: str, target: str, visited: set[str],
             chain: list[str], edge_chain: list[Edge]) -> None:
        if len(chain) > max_hops + 1:
            return
        if current == target and len(chain) >= 2:
            # Вычислить эффективный курс
            effective_rate = 1.0
            min_volume = float("inf")
            for edge in edge_chain:
                effective_rate *= edge.rate
                if edge.is_bank:
                    # Банковская конверсия — практического лимита нет (для алгоритма)
                    avail = 1e12
                elif edge.makers:
                    # Объём: берём max_amount
                    avail = max(m.max_amount for m in edge.makers)
                else:
                    avail = 0
                min_volume = min(min_volume, avail)

            hops = len(chain) - 1
            score = math.log(effective_rate)

            results.append(PathResult(
                chain=list(chain),
                edges=list(edge_chain),
                effective_rate=effective_rate,
                volume=min_volume,
                score=score,
            ))
            return

        visited.add(current)
        for neighbor, edge in adj.get(current, []):
            if neighbor not in visited:
                chain.append(neighbor)
                edge_chain.append(edge)
                _dfs(neighbor, target, visited, chain, edge_chain)
                edge_chain.pop()
                chain.pop()
        visited.discard(current)

    _dfs(source, target, set(), [source], [])
    return results


# ──────────────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────────────


def setup_logging(verbose: bool = False) -> None:
    """Настроить логирование в stderr."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(levelname)s [%(name)s] %(message)s",
        stream=sys.stderr,
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="fx_router — поиск оптимального P2P-маршрута через Bybit",
    )
    parser.add_argument(
        "--amount", type=float, default=100_000.0,
        help="Сумма в RUB (default: 100000)",
    )
    parser.add_argument(
        "--target", type=str, default="USD",
        help="Целевая валюта (default: USD)",
    )
    parser.add_argument(
        "--max-hops", type=int, default=3,
        help="Макс. длина цепочки (default: 3)",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true",
        help="Подробный лог",
    )
    parser.add_argument(
        "--api-key", type=str, default="",
        help="Bybit API key (для V5 P2P). Если не указан — используется публичный эндпоинт.",
    )
    parser.add_argument(
        "--api-secret", type=str, default="",
        help="Bybit API secret (для подписи V5 запросов)",
    )
    parser.add_argument(
        "--pay-in", type=str, default="",
        help="Способы оплаты за крипту (через запятую): ID или подстрока названия, "
             "напр. 'СБП' или '549'. Пример: --pay-in 'Tinkoff,СБП'",
    )
    parser.add_argument(
        "--pay-out", type=str, default="",
        help="Способы ПОЛУЧЕНИЯ фиата (через запятую): ID или подстрока названия, "
             "напр. 'Freedom Bank'. Пример: --pay-out 'Freedom Bank'",
    )
    parser.add_argument(
        "--bank", action="store_true",
        help="Добавить конверсии Freedom Bank (bankffin.kz) как рёбра графа фиат→фиат. "
             "Позволяет искать цепочки вида RUB →(банк)→ KZT →(P2P)→ USDT →(P2P)→ USD",
    )
    parser.add_argument(
        "--bank-section", type=str, default="mobile", choices=["mobile", "cash"],
        help="Тариф банка для --bank: mobile (приложение) или cash (отделение). Default: mobile",
    )
    parser.add_argument(
        "--top", type=int, default=5,
        help="Сколько лучших маршрутов показать (default: 5)",
    )
    parser.add_argument(
        "--pages", type=int, default=None,
        help="Сколько страниц стакана тянуть на пару. 0 = весь стакан (default: 0, весь)",
    )
    return parser.parse_args(argv)


def _payment_names(maker: Maker, id2name: dict[str, str]) -> str:
    """Названия способов оплаты мейкера через запятую."""
    if not maker.payments:
        return ""
    names = [id2name.get(p, f"id:{p}") for p in maker.payments[:4]]
    more = f" +{len(maker.payments) - 4}" if len(maker.payments) > 4 else ""
    return ", ".join(names) + more


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    setup_logging(args.verbose)

    api_key = args.api_key
    api_secret = args.api_secret

    # Опционально подтягиваем .env (если есть python-dotenv)
    if not api_key or not api_secret:
        try:
            from dotenv import load_dotenv  # type: ignore
            load_dotenv()
            api_key = api_key or os.getenv("BYBIT_API_KEY", "")
            api_secret = api_secret or os.getenv("BYBIT_API_SECRET", "")
        except ImportError:
            pass

    cfg = Config(
        amount_rub=args.amount,
        max_hops=args.max_hops,
        bybit_api_key=api_key,
        bybit_api_secret=api_secret,
        pay_in_methods=tuple(s.strip() for s in args.pay_in.split(",") if s.strip()),
        pay_out_methods=tuple(s.strip() for s in args.pay_out.split(",") if s.strip()),
        use_bank_edges=args.bank,
        bank_rates_section=args.bank_section,
    )
    if args.pages is not None:
        cfg.bybit_p2p_pages = args.pages

    source = "RUB"
    target = args.target.upper()

    log.info(
        "fx_router: %s → %s, amount=%.0f RUB, max_hops=%d",
        source, target, cfg.amount_rub, cfg.max_hops,
    )

    # Убедимся, что target в списке
    all_currencies = list(cfg.fiat_currencies) + list(cfg.crypto_currencies)
    if target not in all_currencies:
        log.error("Target currency %s not in currency list", target)
        sys.exit(1)

    log.info("Building graph…")
    edges = build_graph(cfg)

    if not edges:
        log.error("No edges found — cannot compute routes")
        sys.exit(1)

    log.info("Searching paths %s → %s (max %d hops)…", source, target, cfg.max_hops)
    paths = find_paths(source, target, edges, cfg.max_hops, cfg)

    if not paths:
        print(f"No routes found from {source} to {target} (max {cfg.max_hops} hops)")
        sys.exit(0)

    # Сортировка: чисто по выгодности — чем больше target за 1 RUB, тем выше.
    paths.sort(key=lambda p: p.effective_rate, reverse=True)
    top = paths[:max(1, args.top)]

    print()
    # Заголовок: эффективный курс RUB→Target, объём в RUB
    print(f"{'Route':<35} {'Eff.Rate':>10} {'Vol(RUB)':>12} {'Score':>8}  Makers")
    print("-" * 90)
    for p in top:
        route_str = " → ".join(p.chain)
        # Эффективный курс: сколько target за 1 RUB
        makers_str = " → ".join(
            "FreedomBank" if e.is_bank else (e.makers[0].nickname if e.makers else "?")
            for e in p.edges
        )
        # Инвертируем для читаемости: сколько RUB за 1 target
        rub_per_target = 1.0 / p.effective_rate if p.effective_rate > 0 else float("inf")
        print(
            f"{route_str:<35} {rub_per_target:>10.2f} "
            f"{p.volume:>12.0f} {p.score:>8.4f}  {makers_str}"
        )

    print()
    # Детали цепочек
    id2name = load_payment_dict()
    for i, p in enumerate(top, 1):
        rub_per_target = 1.0 / p.effective_rate if p.effective_rate > 0 else float("inf")
        print(f"--- Route #{i}: {' → '.join(p.chain)}  1 {p.chain[-1]} = {rub_per_target:.2f} {p.chain[0]} ---")
        for j, e in enumerate(p.edges):
            if e.is_bank:
                print(f"  Step {j+1}: 1 {e.source} → {e.rate:.6f} {e.target}  [Freedom Bank {cfg.bank_rates_section}]")
                continue
            top3_makers = sorted(e.makers, key=lambda m: m.price, reverse=(e.side == "0"))[:3]
            step_str = f"1 {e.source} → {e.rate:.6f} {e.target}"
            print(f"  Step {j+1}: {step_str}")
            for m in top3_makers:
                pays = _payment_names(m, id2name)
                pays_str = f"  [{pays}]" if pays else ""
                print(f"    {m.price:.2f} — {m.nickname}{pays_str}\n      {m.url}")
            print()


if __name__ == "__main__":
    main()
