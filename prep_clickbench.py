"""One-shot: convert a ClickBench parquet partition to a compact TSV of the
columns the IngoDB benchmark uses.

  curl -sO https://datasets.clickhouse.com/hits_compatible/athena_partitioned/hits_0.parquet
  uv run --with pyarrow python prep_clickbench.py

Output: clickbench_hits.tsv (no header, tab-separated, fixed column order)."""
import datetime as dt
import sys
import pyarrow.parquet as pq

WANT = ["CounterID", "AdvEngineID", "RegionID", "UserID",
        "EventDate", "EventTime", "ResolutionWidth", "SearchPhrase", "Title"]
EPOCH = dt.date(1970, 1, 1)

tbl = pq.read_table("hits_0.parquet")
names = {n.lower(): n for n in tbl.schema.names}
missing = [c for c in WANT if c.lower() not in names]
if missing:
    print("MISSING COLUMNS:", missing, "\nschema:", tbl.schema.names, file=sys.stderr)
    sys.exit(1)
cols = {c: tbl.column(names[c.lower()]).to_pylist() for c in WANT}
n = len(cols["CounterID"])

def as_int(v):
    if v is None:
        return 0
    if isinstance(v, dt.datetime):
        return int(v.timestamp())
    if isinstance(v, dt.date):
        return (v - EPOCH).days
    return int(v)

def as_str(v):
    if v is None:
        return ""
    return str(v).replace("\t", " ").replace("\n", " ").replace("\r", " ")

with open("clickbench_hits.tsv", "w") as f:
    for i in range(n):
        row = [
            as_int(cols["CounterID"][i]),
            as_int(cols["AdvEngineID"][i]),
            as_int(cols["RegionID"][i]),
            as_int(cols["UserID"][i]),
            as_int(cols["EventDate"][i]),
            as_int(cols["EventTime"][i]),
            as_int(cols["ResolutionWidth"][i]),
            as_str(cols["SearchPhrase"][i]),
            as_str(cols["Title"][i]),
        ]
        f.write("\t".join(str(x) for x in row) + "\n")
print(f"wrote clickbench_hits.tsv with {n} rows")
