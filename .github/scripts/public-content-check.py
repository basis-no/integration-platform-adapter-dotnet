#!/usr/bin/env python3
"""Refuse commits that would publish private content in this public repository.

Checks the commits in a range: every added line and every commit message. With
--check-emails it also checks the author and committer addresses, for
maintainers' own pushes; outside contributors may use any address.

The patterns are private and come from a file or an environment variable, never
from this repository. Output names only the location and the pattern number, so
a CI log never reveals a pattern.

usage: public-content-check.py --range BASE..HEAD
       [--patterns-file PATH | patterns in $PUBLIC_CONTENT_PATTERNS] [--check-emails]
"""

import argparse
import os
import re
import subprocess
import sys

ALLOWED_EMAIL = re.compile(r"(@basis\.no|@users\.noreply\.github\.com|^noreply@github\.com)$", re.IGNORECASE)
PRIVATE_IP = re.compile(
    r"\b(10\.\d{1,3}\.\d{1,3}\.\d{1,3}|192\.168\.\d{1,3}\.\d{1,3}|172\.(1[6-9]|2\d|3[01])\.\d{1,3}\.\d{1,3})\b"
)


def git(*args):
    return subprocess.run(["git", *args], check=True, capture_output=True, text=True,
                          encoding="utf-8", errors="replace").stdout


def load_patterns(path):
    text = open(path, encoding="utf-8").read() if path else os.environ.get("PUBLIC_CONTENT_PATTERNS", "")
    patterns = []
    for line in text.splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            patterns.append(re.compile(line, re.IGNORECASE))
    return patterns


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--range", required=True)
    parser.add_argument("--patterns-file")
    parser.add_argument("--check-emails", action="store_true")
    args = parser.parse_args()

    patterns = load_patterns(args.patterns_file)
    if not patterns:
        print("No private patterns configured; refusing to report a clean result.", file=sys.stderr)
        return 2

    def check(text):
        hits = [str(i + 1) for i, p in enumerate(patterns) if p.search(text)]
        if PRIVATE_IP.search(text):
            hits.append("private-ip")
        return hits

    findings = []
    commits = git("rev-list", "--no-merges", args.range).split()
    for commit in commits:
        short = commit[:10]
        author, committer = git("show", "-s", "--format=%ae%n%ce", commit).split("\n")[:2]
        for role, address in (("author", author), ("committer", committer)):
            if args.check_emails and not ALLOWED_EMAIL.search(address):
                findings.append(f"{short} {role} address is not a work or GitHub noreply address")
        for hit in check(git("show", "-s", "--format=%B", commit)):
            findings.append(f"{short} commit message matches pattern {hit}")
        path, line_no = None, 0
        for line in git("show", "--format=", "--unified=0", "--no-color", "--no-ext-diff", commit).splitlines():
            if line.startswith("+++ "):
                path = line[6:] if line.startswith("+++ b/") else line[4:]
            elif line.startswith("@@"):
                match = re.search(r"\+(\d+)", line)
                line_no = int(match.group(1)) if match else 0
            elif line.startswith("+"):
                for hit in check(line[1:]):
                    findings.append(f"{short} {path}:{line_no} matches pattern {hit}")
                line_no += 1

    if findings:
        print("Private content found. Remove it, or rewrite the commits, before this can be published:")
        for finding in findings:
            print("  " + finding)
        return 1
    print(f"Public content check passed for {len(commits)} commit(s).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
