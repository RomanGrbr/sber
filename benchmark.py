import argparse
import json
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed

BASE_URL = ""  # set in main


def post_record(payload: dict) -> float:
    data = json.dumps(payload).encode()
    req = urllib.request.Request(
        f"{BASE_URL}/records",
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    t0 = time.perf_counter()
    with urllib.request.urlopen(req) as resp:
        body = json.loads(resp.read())
    return time.perf_counter() - t0, body["id"]


def get_record(record_id: str) -> float:
    t0 = time.perf_counter()
    with urllib.request.urlopen(f"{BASE_URL}/records/{record_id}") as resp:
        resp.read()
    return time.perf_counter() - t0


def list_records(limit: int = 10, offset: int = 0) -> float:
    t0 = time.perf_counter()
    with urllib.request.urlopen(
        f"{BASE_URL}/records?limit={limit}&offset={offset}"
    ) as resp:
        resp.read()
    return time.perf_counter() - t0


def percentile(sorted_data: list[float], p: float) -> float:
    if not sorted_data:
        return 0.0
    idx = (len(sorted_data) - 1) * p / 100
    lo, hi = int(idx), min(int(idx) + 1, len(sorted_data) - 1)
    return sorted_data[lo] + (sorted_data[hi] - sorted_data[lo]) * (idx - lo)


def print_stats(name: str, latencies: list[float]) -> None:
    s = sorted(latencies)
    total = sum(s)
    rps = len(s) / total if total > 0 else 0
    print(f"\n  {'='*40}")
    print(f"  {name}")
    print(f"  {'='*40}")
    print(f"  requests : {len(s)}")  # количество выполненных запросов
    print(f"  RPS      : {rps:.1f}")  # запросов в секунду
    print(f"  avg      : {total/len(s)*1000:.1f} ms")  # среднее время ответа
    print(f"  p50      : {percentile(s, 50)*1000:.1f} ms")  # 50% запросов быстрее этого значения
    print(f"  p95      : {percentile(s, 95)*1000:.1f} ms")  # 95% запросов быстрее этого значения
    print(f"  p99      : {percentile(s, 99)*1000:.1f} ms")  # 99% запросов быстрее этого значения
    print(f"  min/max  : {s[0]*1000:.1f} / {s[-1]*1000:.1f} ms")  # минимальное и максимальное время ответа


def run_post(n: int, workers: int) -> list[str]:
    latencies: list[float] = []
    ids: list[str] = []
    errors = 0

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [
            pool.submit(post_record, {"data": {"i": i}}) for i in range(n)
        ]
        for fut in as_completed(futures):
            try:
                lat, record_id = fut.result()
                latencies.append(lat)
                ids.append(record_id)
            except Exception:
                errors += 1

    print_stats("POST /records", latencies)
    if errors:
        print(f"  errors   : {errors}")
    return ids


def run_get(ids: list[str], workers: int) -> None:
    latencies: list[float] = []
    errors = 0

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(get_record, rid) for rid in ids]
        for fut in as_completed(futures):
            try:
                latencies.append(fut.result())
            except Exception:
                errors += 1

    print_stats("GET /records/:id", latencies)
    if errors:
        print(f"  errors   : {errors}")


def run_list(n: int, workers: int) -> None:
    latencies: list[float] = []
    errors = 0

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(list_records, 10, 0) for _ in range(n)]
        for fut in as_completed(futures):
            try:
                latencies.append(fut.result())
            except Exception:
                errors += 1

    print_stats("GET /records?limit=10", latencies)
    if errors:
        print(f"  errors   : {errors}")



def main() -> None:
    global BASE_URL

    parser = argparse.ArgumentParser(description="HTTP server benchmark")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--requests", type=int, default=200,
                        help="number of requests per endpoint")
    parser.add_argument("--workers", type=int, default=20,
                        help="concurrent workers")
    args = parser.parse_args()

    BASE_URL = f"http://{args.host}:{args.port}"

    print(f"Target  : {BASE_URL}")
    print(f"Requests: {args.requests}  Workers: {args.workers}")

    ids = run_post(args.requests, args.workers)
    run_get(ids, args.workers)
    run_list(args.requests, args.workers)

    print()


if __name__ == "__main__":
    main()
