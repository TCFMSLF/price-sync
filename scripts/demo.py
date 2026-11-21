#!/usr/bin/env python3
import argparse
import json
import sys
import time
import uuid
from decimal import Decimal, InvalidOperation
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

DEFAULT_ERP_URL = "http://localhost:8000"
DEFAULT_CATALOG_URL = "http://localhost:8001"
DEFAULT_PRODUCT = "SKU-001"
REQUEST_TIMEOUT_SECONDS = 10.0
MAX_PATCH_ATTEMPTS = 3
CATALOG_POLL_INTERVAL_SECONDS = 0.1


class DemoError(RuntimeError):
    pass


class PriceUnchanged(DemoError):
    pass


def _decode_body(raw):
    text = raw.decode("utf-8", errors="replace")
    if not text.strip():
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return text


def _short_body(body):
    if isinstance(body, str):
        return body[:300]
    return json.dumps(body, ensure_ascii=False)[:300]


def request_json(method, url, payload=None):
    data = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        headers["Content-Type"] = "application/json"
    if method.upper() == "PATCH":
        headers["X-Correlation-ID"] = str(uuid.uuid4())

    request = Request(url, data=data, headers=headers, method=method.upper())
    try:
        with urlopen(request, timeout=REQUEST_TIMEOUT_SECONDS) as response:
            return response.status, _decode_body(response.read())
    except HTTPError as error:
        return error.code, _decode_body(error.read())
    except (URLError, OSError, ValueError) as error:
        raise DemoError(f"Сервис недоступен: {url}: {error}") from error


def patch_price(erp_url, product_id, price, expected_version):
    url = f"{erp_url.rstrip('/')}/products/{product_id}/price"
    version = expected_version

    for _attempt in range(MAX_PATCH_ATTEMPTS):
        payload = {
            "newPrice": f"{price:.2f}",
            "expectedPriceVersion": version,
        }
        status, body = request_json("PATCH", url, payload)

        if 200 <= status < 300:
            if not isinstance(body, dict):
                raise DemoError("ERP вернул неожиданный ответ")
            return body

        detail = body.get("detail") if isinstance(body, dict) else None
        if status == 409 and isinstance(detail, dict):
            if "currentPriceVersion" not in detail:
                raise DemoError(f"ERP вернул конфликт без версии: {_short_body(body)}")
            try:
                version = int(detail["currentPriceVersion"])
            except (TypeError, ValueError) as error:
                raise DemoError(f"ERP вернул некорректную версию: {_short_body(body)}") from error
            continue
        if status == 409 and isinstance(detail, str):
            raise PriceUnchanged(detail)
        raise DemoError(f"ERP вернул HTTP {status}: {_short_body(body)}")

    raise DemoError("Не удалось изменить цену: конфликт версии не разрешён за 3 попытки")


def wait_for_catalog(catalog_url, product_id, version, timeout=10.0):
    url = f"{catalog_url.rstrip('/')}/products/{product_id}"
    deadline = time.monotonic() + timeout
    last_status = None
    last_body = None

    while True:
        status, body = request_json("GET", url)
        last_status = status
        last_body = body
        if status == 200 and isinstance(body, dict):
            try:
                current_version = int(body.get("priceVersion", 0))
            except (TypeError, ValueError):
                current_version = 0
            if current_version >= version:
                return body

        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        time.sleep(min(CATALOG_POLL_INTERVAL_SECONDS, remaining))

    raise DemoError(
        f"Каталог не обновился до версии {version} за {timeout:.1f} с; "
        f"последний ответ HTTP {last_status}: {_short_body(last_body)}"
    )


def _parse_price(value, label):
    try:
        return Decimal(str(value))
    except InvalidOperation as error:
        raise DemoError(f"Некорректная {label}: {value}") from error


def _response_version(response):
    try:
        return int(response["priceVersion"])
    except (KeyError, TypeError, ValueError) as error:
        raise DemoError("ERP не вернул priceVersion") from error


def run_change(args):
    price = _parse_price(args.price, "цена")
    started = time.monotonic()

    response = patch_price(args.erp_url, args.product, price, args.expected_version)
    version = _response_version(response)
    print("ERP: " + json.dumps(response, ensure_ascii=False))

    catalog = wait_for_catalog(args.catalog_url, args.product, version)
    elapsed_ms = (time.monotonic() - started) * 1000
    print(f"Каталог: {json.dumps(catalog, ensure_ascii=False)}; ожидание {elapsed_ms:.0f} мс")


def run_measure(args):
    if args.count < 1:
        raise DemoError("Количество изменений должно быть больше нуля")

    price = _parse_price(args.start_price, "стартовая цена")
    expected_version = 1

    for index in range(args.count):
        applied_price = price
        try:
            response = patch_price(
                args.erp_url,
                args.product,
                applied_price,
                expected_version,
            )
        except PriceUnchanged:
            applied_price = price + Decimal("0.01")
            response = patch_price(
                args.erp_url,
                args.product,
                applied_price,
                expected_version,
            )

        version = _response_version(response)
        wait_for_catalog(args.catalog_url, args.product, version)
        expected_version = version
        price = applied_price + Decimal("1.00")
        print(f"{index + 1}/{args.count}: цена {applied_price:.2f}, версия {version}")

    status, body = request_json("GET", f"{args.catalog_url.rstrip('/')}/stats")
    if status != 200 or not isinstance(body, dict):
        raise DemoError(f"Каталог вернул статистику HTTP {status}: {_short_body(body)}")

    latency = body.get("latency")
    if not isinstance(latency, dict):
        raise DemoError("В статистике нет данных о задержке")
    required = {"p95Ms", "avgMs", "maxMs", "sampleSize"}
    if not required.issubset(latency):
        raise DemoError(f"В статистике нет полей задержки: {_short_body(latency)}")

    print("latency: " + json.dumps(latency, ensure_ascii=False))
    print(
        "p95={p95Ms} мс, avg={avgMs:.1f} мс, max={maxMs} мс, sampleSize={sampleSize}".format(
            **latency
        )
    )


def _add_common_arguments(parser):
    parser.add_argument("--erp-url", default=DEFAULT_ERP_URL, help="адрес ERP API")
    parser.add_argument("--catalog-url", default=DEFAULT_CATALOG_URL, help="адрес каталога")
    parser.add_argument("--product", default=DEFAULT_PRODUCT, help="идентификатор товара")


def build_parser():
    parser = argparse.ArgumentParser(description="Демонстрация синхронизации цен")
    subparsers = parser.add_subparsers(dest="command", required=True)

    change = subparsers.add_parser("change", help="изменить цену одного товара")
    _add_common_arguments(change)
    change.add_argument("--price", required=True, help="новая цена, например 1199.00")
    change.add_argument(
        "--expected-version",
        type=int,
        default=1,
        help="ожидаемая версия цены",
    )
    change.set_defaults(handler=run_change)

    measure = subparsers.add_parser("measure", help="измерить задержку синхронизации")
    _add_common_arguments(measure)
    measure.add_argument("--count", type=int, default=30, help="количество изменений")
    measure.add_argument("--start-price", default="900.00", help="стартовая цена")
    measure.set_defaults(handler=run_measure)

    return parser


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        args.handler(args)
    except DemoError as error:
        print(f"Ошибка: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
