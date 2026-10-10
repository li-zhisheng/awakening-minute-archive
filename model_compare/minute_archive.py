"""Build and publish a separate SuperMind one-minute archive.

The archive contains only main-board, non-ST stocks that appeared in the
dashboard signal list. Files are split by trading day and stock so the public
dashboard never needs to download a full-day market bundle.
"""
import argparse
import hashlib
import json
import math
import os
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

try:
    from .execution import is_main_board, is_st
except ImportError:  # Standalone upload in the SuperMind notebook workspace.
    def is_main_board(code):
        text = str(code or "").zfill(6)
        return len(text) == 6 and text.isdigit() and text.startswith(("60", "00"))

    def is_st(name):
        text = str(name or "").upper().replace(" ", "")
        normalized = text.lstrip("*")
        return (
            normalized.startswith("ST")
            or text.startswith(("S*ST", "SST"))
            or "退" in text
        )

REQUEST_SCHEMA = "awakening-minute-archive-request-v1"
STOCK_SCHEMA = "awakening-minute-archive-stock-v1"
MANIFEST_SCHEMA = "awakening-minute-archive-manifest-v1"
PROGRESS_SCHEMA = "awakening-minute-archive-progress-v1"
MINUTE_FIELDS = ["open", "high", "low", "close", "volume", "turnover"]
MIN_BARS = 200
MAX_BATCH_SIZE = 200
BJT = timezone(timedelta(hours=8))


def _digest(value):
    encoded = json.dumps(
        value, ensure_ascii=False, allow_nan=False,
        sort_keys=True, separators=(",", ":"),
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def _atomic_json(path, value):
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(
        prefix=target.name + ".", suffix=".tmp", dir=str(target.parent))
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            json.dump(
                value, stream, ensure_ascii=False, allow_nan=False,
                separators=(",", ":"))
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, target)
    except BaseException:
        try:
            os.unlink(temporary)
        except OSError:
            pass
        raise


def _request_payload(value):
    return {
        key: item for key, item in value.items()
        if key not in ("generated_at", "result_hash")
    }


def _symbol(code):
    return code + (".SH" if code.startswith("6") else ".SZ")


def _stock_path(root, day, code):
    return Path(root) / "days" / day / code[:2] / f"{code}.json"


def build_request_plan(history_dir, output_path):
    """Create a deterministic SuperMind request plan from dashboard history."""
    by_day = {}
    revisions = {}
    for path in sorted(Path(history_dir).glob("*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise ValueError(f"invalid dashboard history file: {path}") from exc
        day = str((payload.get("signals") or {}).get("date") or path.stem)
        if len(day) != 8 or not day.isdigit() or path.stem != day:
            raise ValueError(f"dashboard history date mismatch: {path}")
        stocks = {}
        for row in (payload.get("signals") or {}).get("rows") or ():
            code = str(row.get("code") or "").zfill(6)
            name = str(row.get("name") or code)
            if not is_main_board(code) or is_st(name):
                continue
            stocks[code] = {
                "code": code,
                "symbol": _symbol(code),
                "name": name,
            }
        if stocks:
            by_day[day] = [stocks[code] for code in sorted(stocks)]
            revisions[day] = hashlib.sha256(path.read_bytes()).hexdigest()
    if not by_day:
        raise ValueError("dashboard history has no eligible signal stocks")
    result = {
        "schema": REQUEST_SCHEMA,
        "generated_at": datetime.now(BJT).isoformat(),
        "scope": "main_board_non_st_dashboard_signal_stocks",
        "frequency": "1m",
        "range": [min(by_day), max(by_day)],
        "days": by_day,
        "source_revisions": revisions,
        "request_count": sum(len(rows) for rows in by_day.values()),
    }
    result["result_hash"] = _digest(_request_payload(result))
    _atomic_json(output_path, result)
    return result


def load_request_plan(path):
    value = json.loads(Path(path).read_text(encoding="utf-8"))
    if value.get("schema") != REQUEST_SCHEMA:
        raise ValueError("minute archive request schema mismatch")
    if value.get("result_hash") != _digest(_request_payload(value)):
        raise ValueError("minute archive request hash mismatch")
    days = value.get("days")
    if not isinstance(days, dict) or not days:
        raise ValueError("minute archive request has no days")
    seen = 0
    for day, rows in days.items():
        if len(day) != 8 or not day.isdigit() or not isinstance(rows, list):
            raise ValueError("minute archive request day is invalid")
        codes = []
        for row in rows:
            code = str(row.get("code") or "")
            if (
                    not is_main_board(code)
                    or is_st(str(row.get("name") or ""))
                    or row.get("symbol") != _symbol(code)):
                raise ValueError(f"{day}/{code} is outside archive scope")
            codes.append(code)
        if codes != sorted(set(codes)):
            raise ValueError(f"{day} request codes are duplicated or unsorted")
        seen += len(codes)
    if seen != value.get("request_count"):
        raise ValueError("minute archive request count mismatch")
    return value


def build_export_tasks(request_path, output_path, max_stock_days=2000):
    """Split one request into contiguous, sequentially executable day ranges."""
    request = load_request_plan(request_path)
    if type(max_stock_days) is not int or max_stock_days < MAX_BATCH_SIZE:
        raise ValueError("max_stock_days must be at least 200")
    tasks = []
    current = []
    count = 0
    for day, stocks in sorted(request["days"].items()):
        if current and count + len(stocks) > max_stock_days:
            tasks.append({
                "index": len(tasks),
                "from_day": current[0],
                "through_day": current[-1],
                "stock_days": count,
            })
            current = []
            count = 0
        current.append(day)
        count += len(stocks)
    if current:
        tasks.append({
            "index": len(tasks),
            "from_day": current[0],
            "through_day": current[-1],
            "stock_days": count,
        })
    value = {
        "schema": "awakening-minute-archive-tasks-v1",
        "generated_at": datetime.now(BJT).isoformat(),
        "request": str(request_path),
        "request_hash": request["result_hash"],
        "execution": "sequential",
        "max_stock_days": max_stock_days,
        "task_count": len(tasks),
        "stock_days": sum(task["stock_days"] for task in tasks),
        "tasks": tasks,
    }
    value["result_hash"] = _digest({
        key: item for key, item in value.items()
        if key not in ("generated_at", "result_hash")
    })
    _atomic_json(output_path, value)
    return value


def _records(frame):
    if frame is None or len(frame) == 0:
        return []
    if hasattr(frame, "reset_index") and hasattr(frame, "to_dict"):
        table = frame.reset_index(drop=False)
        stamp_field = table.columns[0]
        return [
            (row.pop(stamp_field), row)
            for row in table.to_dict("records")
        ]
    if isinstance(frame, list):
        output = []
        for row in frame:
            source = dict(row)
            stamp = source.pop("datetime", source.pop("time", None))
            output.append((stamp, source))
        return output
    raise TypeError("minute frame must be a DataFrame or list")


def _minute_stamp(value):
    if hasattr(value, "to_pydatetime"):
        value = value.to_pydatetime()
    if isinstance(value, datetime):
        stamp = value
    else:
        text = str(value or "").strip()
        stamp = None
        for form in (
                "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M",
                "%Y%m%d %H:%M:%S", "%Y%m%d %H:%M"):
            try:
                stamp = datetime.strptime(text, form).replace(tzinfo=BJT)
                break
            except ValueError:
                continue
        if stamp is None:
            raise ValueError("minute timestamp is invalid")
    return stamp.astimezone(BJT) if stamp.tzinfo else stamp.replace(tzinfo=BJT)


def _number(value, field):
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"minute {field} is invalid") from exc
    if not math.isfinite(result):
        raise ValueError(f"minute {field} is invalid")
    return result


def normalize_minute_frame(frame, day, code):
    """Validate a full regular-session frame and return the compact document."""
    points = []
    for raw_stamp, source in _records(frame):
        stamp = _minute_stamp(raw_stamp)
        if stamp.strftime("%Y%m%d") != day:
            raise ValueError("minute date mismatch")
        clock = stamp.strftime("%H%M")
        if not ("0930" <= clock <= "1130" or "1300" <= clock <= "1500"):
            continue
        values = [
            _number(source.get(field), field)
            for field in MINUTE_FIELDS
        ]
        open_, high, low, close, volume, turnover = values
        if (
                min(open_, high, low, close) <= 0
                or high + 1e-9 < max(open_, close)
                or low - 1e-9 > min(open_, close)
                or volume < 0 or turnover < 0):
            raise ValueError("minute OHLCV fields are inconsistent")
        points.append([
            clock, open_, high, low, close, volume, turnover,
        ])
    clocks = [row[0] for row in points]
    if len(points) < MIN_BARS:
        raise ValueError(
            f"minute session is incomplete: {len(points)}<{MIN_BARS}")
    if clocks != sorted(set(clocks)):
        raise ValueError("minute timestamps are duplicated or unsorted")
    document = {
        "schema": STOCK_SCHEMA,
        "date": day,
        "code": code,
        "frequency": "1m",
        "source": "SuperMind get_price 1m",
        "bar_count": len(points),
        "first": clocks[0],
        "last": clocks[-1],
        "points": points,
    }
    document["result_hash"] = _digest(document)
    return document


def validate_stock_document(value, day="", code=""):
    if value.get("schema") != STOCK_SCHEMA:
        raise ValueError("minute stock schema mismatch")
    actual_day = str(value.get("date") or "")
    actual_code = str(value.get("code") or "")
    if day and actual_day != day:
        raise ValueError("minute stock day mismatch")
    if code and actual_code != code:
        raise ValueError("minute stock code mismatch")
    if not is_main_board(actual_code):
        raise ValueError("minute stock is outside main board")
    points = value.get("points")
    if (
            not isinstance(points, list)
            or len(points) < MIN_BARS
            or value.get("bar_count") != len(points)):
        raise ValueError("minute stock bar count mismatch")
    clocks = []
    for row in points:
        if not isinstance(row, list) or len(row) != 7:
            raise ValueError("minute stock row width mismatch")
        clock = str(row[0])
        if not ("0930" <= clock <= "1130" or "1300" <= clock <= "1500"):
            raise ValueError("minute stock timestamp is outside session")
        open_, high, low, close, volume, turnover = [
            _number(item, "archive") for item in row[1:]
        ]
        if (
                min(open_, high, low, close) <= 0
                or high + 1e-9 < max(open_, close)
                or low - 1e-9 > min(open_, close)
                or volume < 0 or turnover < 0):
            raise ValueError("minute stock row is inconsistent")
        clocks.append(clock)
    if clocks != sorted(set(clocks)):
        raise ValueError("minute stock timestamps are duplicated or unsorted")
    declared_hash = value.get("result_hash")
    if declared_hash != _digest({
            key: item for key, item in value.items()
            if key != "result_hash"}):
        raise ValueError("minute stock hash mismatch")
    return value


def _load_progress(root, request_hash):
    path = Path(root) / "progress.json"
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        value = {}
    if value and (
            value.get("schema") != PROGRESS_SCHEMA
            or value.get("request_hash") != request_hash):
        raise ValueError("minute archive progress does not match request")
    return value or {
        "schema": PROGRESS_SCHEMA,
        "request_hash": request_hash,
        "generated_at": None,
        "completed": [],
        "errors": {},
        "calls": 0,
    }


def _write_progress(root, progress):
    progress["generated_at"] = datetime.now(BJT).isoformat()
    _atomic_json(Path(root) / "progress.json", progress)


def export_archive(
        request_path, archive_dir, get_price=None, batch_size=200,
        pause_seconds=1.0, retries=1, progress_callback=None,
        from_day="", through_day=""):
    """Fetch full-session 1m bars with resumable, bounded provider calls."""
    if get_price is None:
        from mindgo_api import get_price as get_price_api
        get_price = get_price_api
    if type(batch_size) is not int or not 1 <= batch_size <= MAX_BATCH_SIZE:
        raise ValueError("batch_size must be between 1 and 200")
    if type(retries) is not int or not 0 <= retries <= 1:
        raise ValueError("retries must be 0 or 1")
    pause_seconds = float(pause_seconds)
    if not math.isfinite(pause_seconds) or not 0 <= pause_seconds <= 60:
        raise ValueError("pause_seconds must be between 0 and 60")

    request = load_request_plan(request_path)
    if from_day and (len(from_day) != 8 or not from_day.isdigit()):
        raise ValueError("from_day must be YYYYMMDD")
    if through_day and (len(through_day) != 8 or not through_day.isdigit()):
        raise ValueError("through_day must be YYYYMMDD")
    selected_days = {
        day: stocks for day, stocks in request["days"].items()
        if (not from_day or day >= from_day)
        and (not through_day or day <= through_day)
    }
    if not selected_days:
        raise ValueError("selected minute archive range has no requests")
    root = Path(archive_dir)
    root.mkdir(parents=True, exist_ok=True)
    progress = _load_progress(root, request["result_hash"])
    completed = set(progress["completed"])
    total = sum(len(stocks) for stocks in selected_days.values())
    done = 0
    for day, stocks in sorted(selected_days.items()):
        for stock in stocks:
            key = day + "/" + stock["code"]
            path = _stock_path(root, day, stock["code"])
            if key not in completed or not path.is_file():
                continue
            try:
                validate_stock_document(
                    json.loads(path.read_text(encoding="utf-8")),
                    day, stock["code"])
                done += 1
            except (OSError, ValueError):
                completed.discard(key)
        for offset in range(0, len(stocks), batch_size):
            chunk = [
                stock for stock in stocks[offset:offset + batch_size]
                if day + "/" + stock["code"] not in completed
            ]
            if not chunk:
                continue
            data = None
            error = None
            for attempt in range(retries + 1):
                try:
                    data = get_price(
                        [stock["symbol"] for stock in chunk],
                        day + " 09:30", day + " 15:00",
                        "1m", MINUTE_FIELDS, False, None, 0, False)
                    progress["calls"] += 1
                    if not isinstance(data, dict):
                        raise TypeError("get_price returned no minute data")
                    error = None
                    break
                except Exception as exc:  # noqa: BLE001 - provider boundary
                    progress["calls"] += 1
                    error = f"{type(exc).__name__}: {exc}"
                    if attempt < retries:
                        time.sleep(max(1.0, pause_seconds))
            if error:
                for stock in chunk:
                    progress["errors"][day + "/" + stock["code"]] = error
            else:
                for stock in chunk:
                    key = day + "/" + stock["code"]
                    try:
                        document = normalize_minute_frame(
                            data.get(stock["symbol"]), day, stock["code"])
                        _atomic_json(
                            _stock_path(root, day, stock["code"]), document)
                    except (KeyError, TypeError, ValueError) as exc:
                        progress["errors"][key] = str(exc)
                        continue
                    completed.add(key)
                    progress["errors"].pop(key, None)
                    done += 1
            progress["completed"] = sorted(completed)
            _write_progress(root, progress)
            if progress_callback:
                progress_callback("minute_archive", done, total)
            if pause_seconds:
                time.sleep(pause_seconds)
    build_manifest(request_path, archive_dir, require_complete=False)
    return progress


def import_live_snapshots(
        request_path, archive_dir, live_dir, day, require_complete=True):
    """Import the Tencent-primary/Sina-backup worker snapshots for one day."""
    request = load_request_plan(request_path)
    if day not in request["days"]:
        raise ValueError("live snapshot day is outside minute archive request")
    iso_day = f"{day[:4]}-{day[4:6]}-{day[6:]}"
    imported = []
    errors = {}
    for stock in request["days"][day]:
        code = stock["code"]
        source_path = Path(live_dir) / code[:2] / f"{code}.json"
        try:
            payload = json.loads(source_path.read_text(encoding="utf-8"))
            if (
                    payload.get("schema") != "awakening-trend-live-stock-v1"
                    or payload.get("code") != code):
                raise ValueError("live minute snapshot schema mismatch")
            source_day = next(
                node for node in payload.get("days") or ()
                if node.get("date") == iso_day)
            points_by_clock = {}
            for row in source_day.get("points") or ():
                if not isinstance(row, list) or len(row) < 4:
                    raise ValueError("live minute row width mismatch")
                clock = str(row[0])
                price = _number(row[1], "price")
                volume = _number(row[2], "volume")
                turnover = _number(row[3], "turnover")
                if (
                        not ("0930" <= clock <= "1130"
                             or "1300" <= clock <= "1500")
                        or price <= 0 or volume < 0 or turnover < 0):
                    raise ValueError("live minute row is invalid")
                points_by_clock[clock] = [
                    clock, price, price, price, price, volume, turnover,
                ]
            points = [
                points_by_clock[clock] for clock in sorted(points_by_clock)
            ]
            clocks = [row[0] for row in points]
            if len(points) < MIN_BARS or clocks != sorted(set(clocks)):
                raise ValueError("live minute session is incomplete")
            document = {
                "schema": STOCK_SCHEMA,
                "date": day,
                "code": code,
                "frequency": "1m",
                "source": (
                    source_day.get("source")
                    or "Tencent batch primary / Sina batch backup"),
                "bar_count": len(points),
                "first": clocks[0],
                "last": clocks[-1],
                "points": points,
            }
            document["result_hash"] = _digest(document)
            validate_stock_document(document, day, code)
            _atomic_json(_stock_path(archive_dir, day, code), document)
            imported.append(code)
        except (StopIteration, OSError, TypeError, ValueError) as exc:
            errors[code] = str(exc)
    if require_complete and len(imported) != len(request["days"][day]):
        raise RuntimeError(
            f"{day} live minute archive is incomplete: "
            f"imported={len(imported)} expected={len(request['days'][day])} "
            f"errors={list(errors)[:5]}")
    manifest = build_manifest(
        request_path, archive_dir, require_complete=False)
    return {
        "date": day,
        "expected": len(request["days"][day]),
        "imported": len(imported),
        "errors": errors,
        "manifest_status": manifest["status"],
    }


def build_manifest(request_path, archive_dir, require_complete=True):
    """Validate every archive file and write the public manifest."""
    request = load_request_plan(request_path)
    root = Path(archive_dir)
    days = {}
    missing = []
    invalid = {}
    for day, stocks in sorted(request["days"].items()):
        codes = []
        bars = 0
        for stock in stocks:
            code = stock["code"]
            path = _stock_path(root, day, code)
            if not path.is_file():
                missing.append(day + "/" + code)
                continue
            try:
                value = validate_stock_document(
                    json.loads(path.read_text(encoding="utf-8")), day, code)
            except (OSError, ValueError) as exc:
                invalid[day + "/" + code] = str(exc)
                continue
            codes.append(code)
            bars += value["bar_count"]
        days[day] = {
            "expected": len(stocks),
            "available": len(codes),
            "bar_count": bars,
            "codes": codes,
        }
    complete = not missing and not invalid
    manifest = {
        "schema": MANIFEST_SCHEMA,
        "generated_at": datetime.now(BJT).isoformat(),
        "status": "complete" if complete else "partial",
        "frequency": "1m",
        "source": "SuperMind get_price 1m",
        "request_hash": request["result_hash"],
        "range": request["range"],
        "expected_count": request["request_count"],
        "available_count": sum(node["available"] for node in days.values()),
        "missing_count": len(missing),
        "invalid_count": len(invalid),
        "days": days,
    }
    manifest["result_hash"] = _digest({
        key: value for key, value in manifest.items()
        if key not in ("generated_at", "result_hash")
    })
    _atomic_json(root / "manifest.json", manifest)
    if require_complete and not complete:
        detail = (
            f"missing={len(missing)} invalid={len(invalid)} "
            f"examples={(missing + list(invalid))[:5]}")
        raise RuntimeError("minute archive is incomplete: " + detail)
    return manifest


def publish_archive(request_path, archive_dir, repo="awakening-minute-archive"):
    """Publish only a strictly complete archive to its independent Pages repo."""
    manifest = build_manifest(
        request_path, archive_dir, require_complete=True)
    nojekyll = Path(archive_dir) / ".nojekyll"
    nojekyll.touch(exist_ok=True)
    from .deploy_pages import deploy_public
    result = deploy_public(str(archive_dir), name=repo)
    if result.get("error"):
        raise RuntimeError("minute archive publish failed: " + result["error"])
    return {"manifest": manifest, **result}


def main():
    parser = argparse.ArgumentParser(
        description="SuperMind historical one-minute archive")
    subparsers = parser.add_subparsers(dest="command", required=True)
    plan = subparsers.add_parser("plan")
    plan.add_argument("--history-dir", required=True)
    plan.add_argument("--output", required=True)
    tasks = subparsers.add_parser("tasks")
    tasks.add_argument("--request", required=True)
    tasks.add_argument("--output", required=True)
    tasks.add_argument("--max-stock-days", type=int, default=2000)
    export = subparsers.add_parser("export")
    export.add_argument("--request", required=True)
    export.add_argument("--archive-dir", required=True)
    export.add_argument("--batch-size", type=int, default=200)
    export.add_argument("--pause-seconds", type=float, default=1.0)
    export.add_argument("--retries", type=int, default=1)
    export.add_argument("--from-day", default="")
    export.add_argument("--through-day", default="")
    live = subparsers.add_parser("import-live")
    live.add_argument("--request", required=True)
    live.add_argument("--archive-dir", required=True)
    live.add_argument("--live-dir", required=True)
    live.add_argument("--day", required=True)
    validate = subparsers.add_parser("validate")
    validate.add_argument("--request", required=True)
    validate.add_argument("--archive-dir", required=True)
    publish = subparsers.add_parser("publish")
    publish.add_argument("--request", required=True)
    publish.add_argument("--archive-dir", required=True)
    publish.add_argument("--repo", default="awakening-minute-archive")
    args = parser.parse_args()
    if args.command == "plan":
        result = build_request_plan(args.history_dir, args.output)
        summary = {
            "schema": result["schema"],
            "range": result["range"],
            "days": len(result["days"]),
            "request_count": result["request_count"],
            "result_hash": result["result_hash"],
            "output": args.output,
        }
    elif args.command == "tasks":
        result = build_export_tasks(
            args.request, args.output,
            max_stock_days=args.max_stock_days)
        summary = {
            "schema": result["schema"],
            "task_count": result["task_count"],
            "stock_days": result["stock_days"],
            "execution": result["execution"],
            "output": args.output,
        }
    elif args.command == "export":
        result = export_archive(
            args.request, args.archive_dir,
            batch_size=args.batch_size,
            pause_seconds=args.pause_seconds,
            retries=args.retries,
            from_day=args.from_day,
            through_day=args.through_day,
            progress_callback=lambda stage, done, total: print(
                f"{stage}: {done}/{total}", flush=True),
        )
        summary = {
            "completed": len(result["completed"]),
            "errors": len(result["errors"]),
            "calls": result["calls"],
            "archive_dir": args.archive_dir,
        }
    elif args.command == "import-live":
        result = import_live_snapshots(
            args.request, args.archive_dir, args.live_dir, args.day)
        summary = result
    elif args.command == "validate":
        result = build_manifest(
            args.request, args.archive_dir, require_complete=True)
        summary = {
            key: result[key] for key in (
                "status", "range", "expected_count", "available_count",
                "missing_count", "invalid_count", "result_hash")
        }
    else:
        result = publish_archive(
            args.request, args.archive_dir, repo=args.repo)
        summary = {
            "url": result["url"],
            "status": result["manifest"]["status"],
            "available_count": result["manifest"]["available_count"],
        }
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
