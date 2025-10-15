#!/usr/bin/env python3
"""
rtab_to_csv.py
Convert an .rtab text table to CSV. Tries to detect delimiter (tab/semicolon/comma/space),
handles gzip (.rtab.gz), shows a short preview, and writes a proper CSV.
Requires: pandas (recommended). Falls back to csv+sniff if pandas not available.
"""
import sys
import os
import gzip
import io
import csv

def open_maybe_gzip(path):
    if path.endswith(".gz"):
        return io.TextIOWrapper(gzip.open(path, "rb"), encoding="utf-8", errors="replace")
    return open(path, "r", encoding="utf-8", errors="replace")

def detect_delimiter(sample):
    # Common delimiters to test (tab first)
    delims = ["\t", ";", ",", "|", " "]
    lines = sample.strip().splitlines()
    best = delims[0]
    best_score = -1
    for d in delims:
        counts = [line.count(d) for line in lines if line.strip()]
        if not counts:
            continue
        # score: low variance, reasonably large mean count
        mean = sum(counts)/len(counts)
        var = sum((c-mean)**2 for c in counts)/len(counts)
        score = mean - var
        if score > best_score:
            best_score = score
            best = d
    return best

def read_preview(path, n=10):
    with open_maybe_gzip(path) as f:
        sample_lines = []
        for _ in range(n):
            line = f.readline()
            if not line:
                break
            sample_lines.append(line)
    return "".join(sample_lines)

def convert_with_pandas(inpath, outpath, delimiter, comment):
    import pandas as pd
    # read with pandas
    df = pd.read_csv(inpath, sep=delimiter, comment=comment, engine="python", dtype=str)
    # optionally strip column names of leading/trailing whitespace:
    df.columns = [c.strip() if isinstance(c, str) else c for c in df.columns]
    df.to_csv(outpath, index=False, encoding="utf-8")
    return df

def convert_fallback(inpath, outpath, delimiter, comment):
    # csv module fallback: streaming copy; assumes header present
    with open_maybe_gzip(inpath) as fin, open(outpath, "w", newline="", encoding="utf-8") as fout:
        reader = csv.reader(fin, delimiter=delimiter)
        writer = csv.writer(fout)
        for row in reader:
            # skip comment lines
            if row and isinstance(row[0], str) and row[0].lstrip().startswith(comment):
                continue
            writer.writerow(row)

def main():
    if len(sys.argv) < 3:
        print("Usage: python rtab_to_csv.py input.rtab output.csv")
        sys.exit(1)
    inpath, outpath = sys.argv[1], sys.argv[2]
    if not os.path.exists(inpath):
        print("Input file not found:", inpath)
        sys.exit(2)

    sample = read_preview(inpath, n=20)
    print("=== file preview ===")
    print(sample if sample else "(file empty)")
    delim = detect_delimiter(sample)
    print(f"Detected delimiter: {repr(delim)}")
    comment_char = "#"  # change if your rtab uses other comment prefix

    try:
        print("Trying to convert with pandas...")
        df = convert_with_pandas(inpath, outpath, delim, comment_char)
        print(f"Wrote {outpath} with {len(df)} rows and {len(df.columns)} columns.")
    except Exception as e:
        print("Pandas conversion failed (or pandas not installed). Falling back to csv module. Error:", e)
        try:
            convert_fallback(inpath, outpath, delim, comment_char)
            print("Wrote", outpath)
        except Exception as e2:
            print("Fallback conversion also failed:", e2)
            sys.exit(3)

if __name__ == "__main__":
    main()
