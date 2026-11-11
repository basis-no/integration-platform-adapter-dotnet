"""Tests for public-content-check.py. Run: python3 -I -B -m unittest discover -s .github/scripts"""

import os
import subprocess
import sys
import tempfile
import unittest

SCRIPT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "public-content-check.py")


def isolated_environment():
    """The caller's environment without its git settings: a GIT_DIR from a hook
    would point git at the caller's repository, and a developer's own config
    could change the output the script parses."""
    env = {name: value for name, value in os.environ.items() if not name.startswith("GIT_")}
    env.update(GIT_AUTHOR_NAME="Synthetic", GIT_AUTHOR_EMAIL="synthetic@users.noreply.github.com",
               GIT_COMMITTER_NAME="Synthetic", GIT_COMMITTER_EMAIL="synthetic@users.noreply.github.com",
               GIT_CONFIG_GLOBAL=os.devnull, GIT_CONFIG_NOSYSTEM="1")
    return env


class CommitRangeTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.repo = self.directory.name
        self.git("init", "--quiet", "--initial-branch=main")

    def tearDown(self):
        self.directory.cleanup()

    def git(self, *args, date=None):
        env = isolated_environment()
        if date is not None:
            env["GIT_AUTHOR_DATE"] = env["GIT_COMMITTER_DATE"] = f"@{date} +0000"
        return subprocess.run(["git", *args], cwd=self.repo, env=env, check=True,
                              capture_output=True, text=True).stdout.strip()

    def commit(self, name, text, date):
        with open(os.path.join(self.repo, name), "w", encoding="utf-8") as file:
            file.write(text + "\n")
        self.git("add", name)
        self.git("commit", "--quiet", "-m", f"Add {name}", date=date)
        return self.git("rev-parse", "HEAD")

    def check(self, commit_range):
        patterns = os.path.join(self.repo, ".patterns")
        with open(patterns, "w", encoding="utf-8") as file:
            file.write("already-published\n")
        return subprocess.run([sys.executable, "-I", "-B", SCRIPT, "--range", commit_range, "--patterns-file", patterns],
                              cwd=self.repo, env=isolated_environment(), capture_output=True, text=True)

    def test_a_commit_dated_before_its_parent_does_not_bring_published_commits_into_the_range(self):
        # The published root holds a line the patterns match. The base branch
        # continues from it with commits dated before it, so git's date-ordered
        # walk from BASE..HEAD stops before it reaches the root through them
        # (git 2.55 needs seven; twenty leave a margin).
        root = self.commit("published.txt", "already-published", date=200)
        self.git("checkout", "--quiet", "-b", "base")
        for offset in range(20):
            base = self.commit(f"base-{offset}.txt", "base", date=10 + offset)
        self.git("checkout", "--quiet", "-b", "head", root)
        head = self.commit("new.txt", "new", date=300)

        result = self.check(f"{base}..{head}")

        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertIn("passed for 1 commit(s)", result.stdout)

    def test_a_single_commit_range_still_checks_its_whole_history(self):
        self.commit("published.txt", "already-published", date=200)
        head = self.commit("new.txt", "new", date=300)

        result = self.check(head)

        self.assertEqual(1, result.returncode)
        self.assertIn("published.txt:1 matches pattern 1", result.stdout)


if __name__ == "__main__":
    unittest.main()
