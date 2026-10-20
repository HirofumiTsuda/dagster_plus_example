"""Time assetNodes with and without staleStatus for growing prefixes of the stale_scaling graph.

Needs the stale_scaling assets deployed and materialized (see README). Reads the endpoint
and token from DAGSTER_GRAPHQL_ENDPOINT and DAGSTER_CLOUD_API_TOKEN; never prints the token.

    python scripts/measure_stale_status.py [--layers 20] [--per-layer 50] [--repeats 3]
"""
import argparse
import json
import os
import statistics
import time
import urllib.request


def post(endpoint: str, token: str, query: str, variables: dict) -> tuple[float, dict]:
    req = urllib.request.Request(
        endpoint,
        data=json.dumps({"query": query, "variables": variables}).encode(),
        headers={"Content-Type": "application/json", "Dagster-Cloud-Api-Token": token},
    )
    start = time.monotonic()
    with urllib.request.urlopen(req, timeout=180) as resp:
        body = json.load(resp)
    return time.monotonic() - start, body


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--layers", type=int, default=20)
    ap.add_argument("--per-layer", type=int, default=50)
    ap.add_argument("--repeats", type=int, default=3)
    ap.add_argument("--steps", default="2,4,6,10,14,20", help="layer prefixes to measure")
    args = ap.parse_args()

    endpoint = os.environ["DAGSTER_GRAPHQL_ENDPOINT"]
    token = os.environ["DAGSTER_CLOUD_API_TOKEN"]
    plain = "query($k:[AssetKeyInput!]){assetNodes(assetKeys:$k){assetKey{path} isPartitioned}}"
    stale = "query($k:[AssetKeyInput!]){assetNodes(assetKeys:$k){assetKey{path} isPartitioned staleStatus}}"

    print(f"{'assets':>7} {'plain median':>13} {'stale median':>13} {'extra':>8} {'per asset':>10}  statuses")
    for layers in (int(x) for x in args.steps.split(",") if int(x) <= args.layers):
        keys = [
            {"path": [f"scale_l{layer:02d}_a{i:02d}"]}
            for layer in range(layers)
            for i in range(args.per_layer)
        ]
        t_plain, t_stale, statuses = [], [], {}
        for _ in range(args.repeats):
            t, body = post(endpoint, token, plain, {"k": keys})
            assert "errors" not in body, body.get("errors")
            t_plain.append(t)
            t, body = post(endpoint, token, stale, {"k": keys})
            assert "errors" not in body, body.get("errors")
            t_stale.append(t)
            statuses = {}
            for n in body["data"]["assetNodes"]:
                statuses[n["staleStatus"]] = statuses.get(n["staleStatus"], 0) + 1
        p, s = statistics.median(t_plain), statistics.median(t_stale)
        print(f"{len(keys):>7} {p:>12.2f}s {s:>12.2f}s {s - p:>7.2f}s {1000 * (s - p) / len(keys):>8.1f}ms  {statuses}")


if __name__ == "__main__":
    main()
