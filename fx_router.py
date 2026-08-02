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
    bybit_p2p_pages: int = 10               # сколько последних страниц запрашивать (там самые дешёвые)

    # Фильтры мейкеров
    min_recent_order_num: int = 100
    min_recent_execute_rate: float = 99.0
    antiscam_threshold: float = 0.02  # отклонение от медианы ВСЕХ валидных (+2%)
    max_ad_age_hours: float = 336.0   # объявления старше 14 дней — мусор (цены неактуальны)
    blacklist_keywords: tuple[str, ...] = ("pyypl", "доверенн")

    # Поиск путей
    max_hops: int = 3
    amount_rub: float = 100_000.0  # сумма сделки в RUB

    # Скоринг
    hop_penalty: float = 0.005  # log-штраф за каждое ребро сверх двух

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
    makers: list[Maker]  # мейкеры, по которым считали
    side: str            # "0" — юзер покупает крипту, "1" — продаёт


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
    side: str,  # "0" — юзер покупает, "1" — продаёт
    cfg: Config,
    client: httpx.Client,
) -> list[dict[str, Any]]:
    """Запросить P2P-объявления Bybit.

    Раньше скрипт тянул ПОСЛЕДНИЕ страницы в надежде найти самые дешёвые
    объявления. На практике сортировка api2.bybit.com нестабильна
    (иногда дешёвые сначала, иногда с конца), а в конце стакана копятся
    годами не обновлявшиеся объявления с неактуальными ценами — на них
    скрипт и покупался. Поэтому берём первые N страниц и полагаемся на
    фильтры свежести и анти-скам в filter_makers().
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

    # Берём первые N страниц с начала (не с конца!)
    pages_to_fetch = min(cfg.bybit_p2p_pages, total_pages)

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
# Фильтры мейкеров
# ──────────────────────────────────────────────────────────────────────


def filter_makers(
    makers: list[Maker],
    side: str,         # "0" — покупаем крипту, "1" — продаём
    amount: float,     # сумма сделки в фиате объявления (0 = пропустить лимитную проверку)
    cfg: Config,
) -> list[Maker]:
    """Отфильтровать мейкеров по критериям качества.

    Если ``amount`` == 0, проверка лимитов (minAmount/maxAmount) пропускается —
    это нужно при построении графа, когда реальный объём ещё неизвестен.

    * ``recentOrderNum >= min_recent_order_num``
    * ``recentExecuteRate >= min_recent_execute_rate``
    * мейкер онлайн
    * лимиты объявления вмещают сумму
    * условие не содержит blacklist-подстрок
    * объявление свежее ``max_ad_age_hours`` (иначе цена неактуальна)
    * анти-скам: курс не отклоняется от медианы ВСЕХ валидных больше чем на ``antiscam_threshold``
    """
    total = len(makers)
    now_ms = time.time() * 1000

    # Базовые фильтры
    valid: list[Maker] = []
    for m in makers:
        if not m.is_online:
            continue
        if m.recent_order_num < cfg.min_recent_order_num:
            continue
        if m.recent_execute_rate < cfg.min_recent_execute_rate:
            continue
        if amount and (amount < m.min_amount or amount > m.max_amount):
            continue
        # Свежесть: старые объявления висят годами с ценами из другой эпохи
        if m.create_date_ms > 0 and (now_ms - m.create_date_ms) > cfg.max_ad_age_hours * 3600_000:
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


def _edge_rate(makers: list[Maker]) -> float:
    """Курс ребра = медиана топ-3 по цене для мейкера."""
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
                # Фиат→крипта: проверяем сумму только если source — RUB
                amt = cfg.amount_rub if src == "RUB" else 0.0
                pairs.append((src, dst, src, dst, "0", amt))
            elif src in cryptos and dst in fiats:
                # Крипта→фиат: сумма в крипте неизвестна до построения пути
                pairs.append((src, dst, dst, src, "1", 0.0))
            # else: skip fiat↔️fiat or crypto↔️crypto

    def _fetch_one(pair: tuple[str, str, str, str, str, float]) -> tuple[tuple[str, str], Edge] | None:
        """Запросить и отфильтровать одну пару. Вызывается в ThreadPool."""
        src, dst, fiat_currency, token_id, side, step_amount = pair
        with httpx.Client() as client:
            raw = fetch_ads(token_id, fiat_currency, side, cfg, client)
        makers = parse_makers(raw)
        if not makers:
            return None
        valid = filter_makers(makers, side, step_amount, cfg)
        if len(valid) < 3:
            return None
        raw_rate = _edge_rate(valid)
        if side == "0":
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
                # Объём: берём max_amount
                avail = max(m.max_amount for m in edge.makers) if edge.makers else 0
                min_volume = min(min_volume, avail)

            hops = len(chain) - 1
            score = math.log(effective_rate) - cfg.hop_penalty * max(0, hops - 2)

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
    return parser.parse_args(argv)


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
    )

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

    # Сортировка по score убыванию, топ-3
    paths.sort(key=lambda p: p.score, reverse=True)
    top = paths[:3]

    print()
    # Заголовок: эффективный курс RUB→Target, объём в RUB
    print(f"{'Route':<35} {'Eff.Rate':>10} {'Vol(RUB)':>12} {'Score':>8}  Makers")
    print("-" * 90)
    for p in top:
        route_str = " → ".join(p.chain)
        # Эффективный курс: сколько target за 1 RUB
        makers_str = " → ".join(
            f"{e.makers[0].nickname if e.makers else '?'}"
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
    for i, p in enumerate(top, 1):
        rub_per_target = 1.0 / p.effective_rate if p.effective_rate > 0 else float("inf")
        print(f"--- Route #{i}: {' → '.join(p.chain)}  1 {p.chain[-1]} = {rub_per_target:.2f} {p.chain[0]} ---")
        for j, e in enumerate(p.edges):
            top3_makers = sorted(e.makers, key=lambda m: m.price)[:3]
            step_str = f"1 {e.source} → {e.rate:.6f} {e.target}"
            print(f"  Step {j+1}: {step_str}")
            for m in top3_makers:
                print(f"    {m.price:.2f} — {m.nickname}\n      {m.url}")
            print()


if __name__ == "__main__":
    main()
