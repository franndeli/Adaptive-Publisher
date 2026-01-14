import json
import requests
import pandas as pd

BASE = "http://localhost:16686"
SERVICE = "AdaptivePublisher"   # exact zoals in /api/services
LIMIT = 10000

# --- CONFIG ---
INPUT_JSON = "traces_AdaptivePublisher.json"   # pas aan naar jouw exportbestand
OPERATION = "encode_and_store_frame"

def get_services():
    r = requests.get(f"{BASE}/api/services", timeout=10)
    r.raise_for_status()
    return r.json()["data"]

def fetch_traces(service: str, limit=1000):
    params = {
        "service": service,
        "limit": limit,
    }
    r = requests.get(f"{BASE}/api/traces", params=params, timeout=30)
    r.raise_for_status()
    return r.json()["data"]

def spans_to_df(traces):
    rows = []
    for t in traces:
        trace_id = t.get("traceID")
        for sp in t.get("spans", []):
            if sp.get("operationName") != OPERATION:
                continue

            tags = {x["key"]: x.get("value") for x in sp.get("tags", [])}

            rows.append({
                "trace_id": trace_id,
                "span_id": sp.get("spanID"),
                "frame_index": tags.get("frame.index"),
                "image_mode": tags.get("image.mode"),
                "encode_ms": tags.get("encode.ms"),                 # for png/webp/lz4
                "upload_ms": tags.get("upload.ms"),                 # for png/webp/lz4
                "baseline_upload_ms": tags.get("baseline.upload.ms"),# for baseline
                "payload_bytes": tags.get("payload.bytes"),         # for png/webp/lz4 (baseline often missing)
            })

    df = pd.DataFrame(rows)

    # Convert to numeric where possible
    for c in ["frame_index", "encode_ms", "upload_ms", "baseline_upload_ms", "payload_bytes"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # Baseline uses baseline.upload.ms; other modes use upload.ms
    df["effective_upload_ms"] = df["upload_ms"].fillna(df["baseline_upload_ms"])

    # baseline doesn't set encode.ms in your code; keep it NaN
    return df

def summarize(df):
    # only keep rows where image_mode exists
    df = df.dropna(subset=["image_mode"]).copy()

    def p95(s):
        s = s.dropna()
        return float(s.quantile(0.95)) if len(s) else float("nan")

    summary = (
        df.groupby("image_mode")
          .agg(
              frames=("trace_id", "count"),
              encode_ms_mean=("encode_ms", "mean"),
              encode_ms_p95=("encode_ms", p95),
              upload_ms_mean=("effective_upload_ms", "mean"),
              upload_ms_p95=("effective_upload_ms", p95),
              payload_kb_mean=("payload_bytes", lambda s: (s.dropna().mean() / 1024.0) if s.dropna().size else float("nan")),
              payload_kb_p95=("payload_bytes", lambda s: (p95(s) / 1024.0) if s.dropna().size else float("nan")),
          )
          .sort_values("frames", ascending=False)
    )
    return summary

def main():
    services = get_services()
    print("Services in Jaeger:", services)

    if SERVICE not in services:
        raise RuntimeError(f"Service '{SERVICE}' not found in Jaeger")

    traces = fetch_traces(SERVICE, limit=LIMIT)
    print(f"Fetched {len(traces)} traces for service '{SERVICE}'")

    out = f"traces_{SERVICE}.json"
    with open(out, "w", encoding="utf-8") as f:
        json.dump(traces, f)

    print(f"Saved traces to {out}")
    with open(INPUT_JSON, "r", encoding="utf-8") as f:
        traces = json.load(f)

    print(f"Loaded traces: {len(traces)} from {INPUT_JSON}")

    df = spans_to_df(traces)
    print(f"Extracted spans '{OPERATION}': {len(df)}")

    if df.empty:
        print("No matching spans found. Check OPERATION name or ensure that code path ran.")
        return

    # Save raw rows for your own digging
    df.to_csv("spans_by_image_mode.csv", index=False)
    print("Wrote spans_by_image_mode.csv")

    summary = summarize(df)
    print("\n=== Summary by image_mode ===")
    print(summary.round(3))

    summary.to_csv("summary_by_image_mode.csv")
    print("\nWrote summary_by_image_mode.csv")

    # Quick direct baseline vs lz4 comparison if both exist
    modes = set(df["image_mode"].dropna().unique())
    if "baseline" in modes and "lz4_lossless" in modes:
        sub = summary.loc[["baseline", "lz4_lossless"]]
        print("\n=== Baseline vs LZ4 ===")
        print(sub.round(3))
    else:
        print("\nNote: baseline and/or lz4_lossless not both present in this export.")

if __name__ == "__main__":
    main()
