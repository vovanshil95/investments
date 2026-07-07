# Investments — анализ облигаций Московской биржи

Пайплайн для выкачки и анализа облигаций через публичное API Т‑Банка (Тинькофф Инвестиции).
Стратегия — поиск бумаг с **ежемесячным купоном**: 12 выплат в год, купон ≥ 2% от цены, цена ≥ 97% номинала.

## Пайплайн

```
fetch_bonds.py  ─►  fetch_bonds_detail.py  ─►  filter_bonds.py  ─►  bonds_analysis.ipynb
   тикеры              детали (~43 поля)          шортлист            анализ + графики
```

| Скрипт | Вход | Выход | Что делает |
|---|---|---|---|
| `fetch_bonds.py` | — | `bonds_tickers.csv` | Список всех облигаций (постранично) |
| `fetch_bonds_detail.py` | `bonds_tickers.csv` | `bonds_detail.csv` | Детали по каждой бумаге. Resume, случайная задержка против троттлинга |
| `filter_bonds.py` | `bonds_detail.csv` | `bonds_filtered.csv` | Фильтр под стратегию (ежемесячный купон) |
| `bonds_analysis.ipynb` | `bonds_filtered.csv` | `bonds_analysis.html` | Топы по группам надёжности, графики |

## Запуск

```bash
pip install -r requirements.txt

python fetch_bonds.py            # -> bonds_tickers.csv
python fetch_bonds_detail.py     # -> bonds_detail.csv (долго, ~25 мин)
python filter_bonds.py           # -> bonds_filtered.csv

jupyter nbconvert --to notebook --execute --inplace bonds_analysis.ipynb
jupyter nbconvert --to html bonds_analysis.ipynb   # -> bonds_analysis.html
```

## Данные

- `bonds_tickers.csv` — сырой список бумаг.
- `bonds_detail.csv` — полные детали (цены, купон, доходность, дюрация, риск, ликвидность и т.д.).
- `bonds_filtered.csv` — шортлист под стратегию (+ колонка `securitization`).
- `bonds_secured.csv` — облигации с залоговым обеспечением (секьюритизация / эмитент‑СФО).

## Заметки

- API Т‑Банка публичное, авторизация не нужна.
- Поле `securitizationFlag` в API **неполное**: у части выпусков СФО стоит `False`. Поэтому залоговые бумаги отбираются также по имени эмитента «СФО» (Специализированное финансовое общество).
- CSV — это снимок рынка на момент выкачки, а не актуальные котировки.

> ⚠️ Репозиторий для личного анализа. Ничто здесь не является индивидуальной инвестиционной рекомендацией.
