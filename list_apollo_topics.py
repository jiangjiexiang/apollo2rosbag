#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import os
import sys
from collections import Counter

from cyber_record.record import Record


def collect_record_files(input_path):
    if os.path.isfile(input_path):
        return [input_path]

    files = []
    for name in sorted(os.listdir(input_path)):
        path = os.path.join(input_path, name)
        if os.path.isfile(path):
            files.append(path)
    return files


def list_topics_from_index(record_file):
    topics = Counter()
    with Record(record_file) as record:
        for channel in record.get_channel_cache():
            topics[channel.name] += channel.message_number
    return topics


def list_topics_by_scan(record_file, limit=None):
    topics = Counter()
    scanned = 0
    with Record(record_file) as record:
        for topic, message, timestamp in record.read_messages():
            topics[topic] += 1
            scanned += 1
            if limit is not None and scanned >= limit:
                break
    return topics


def parse_args():
    parser = argparse.ArgumentParser(description="List topics in Apollo Cyber record files.")
    parser.add_argument("input", help="Apollo .record file or directory containing record files")
    parser.add_argument("--scan", action="store_true", help="Scan messages instead of using the record index")
    parser.add_argument("--limit", type=int, default=None, help="Maximum messages to scan per file with --scan")
    parser.add_argument("--per-file", action="store_true", help="Print topics for each record file")
    return parser.parse_args()


def main():
    args = parse_args()

    if not os.path.exists(args.input):
        print(f"[FATAL] Input path not found: {args.input}", file=sys.stderr)
        return 1

    record_files = collect_record_files(args.input)
    if not record_files:
        print(f"[FATAL] No files found: {args.input}", file=sys.stderr)
        return 1

    total_topics = Counter()
    failed = []

    for record_file in record_files:
        try:
            if args.scan:
                topics = list_topics_by_scan(record_file, limit=args.limit)
            else:
                topics = list_topics_from_index(record_file)
        except Exception as exc:
            failed.append((record_file, str(exc)))
            continue

        total_topics.update(topics)

        if args.per_file:
            print(record_file)
            for topic, count in sorted(topics.items()):
                print(f"  {count}\t{topic}")

    print(f"files: {len(record_files)}")
    print(f"topics: {len(total_topics)}")
    for topic, count in sorted(total_topics.items()):
        print(f"{count}\t{topic}")

    if failed:
        print("\nfailed files:", file=sys.stderr)
        for record_file, error in failed:
            print(f"  {record_file}: {error}", file=sys.stderr)

    return 1 if failed and not total_topics else 0


if __name__ == "__main__":
    sys.exit(main())

