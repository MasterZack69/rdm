import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

SCRIPT = Path(__file__).with_name("download_gate.py")


class GateTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.secret_url = "https://example.test/file?token=PRIVATE_SECRET"
        (self.root / "url.txt").write_text(self.secret_url)
        self.fake = self.root / "fake.py"
        self.fake.write_text("import pathlib,sys\npathlib.Path(sys.argv[2]).write_bytes(b'correct')\n")
        self.cfg = {
            "targets": [{"label": "example", "url_file": str(self.root / "url.txt"),
                         "size_bytes": 7, "sha256": hashlib.sha256(b"correct").hexdigest()}],
            "tools": [{"label": label, "argv": [sys.executable, str(self.fake), "{url}", "{output}"]}
                      for label in ("base", "candidate")],
        }

    def tearDown(self):
        self.tmp.cleanup()

    def run_gate(self, rounds=2, timeout=5):
        config = self.root / "config.json"
        output = self.root / "results.json"
        config.write_text(json.dumps(self.cfg))
        p = subprocess.run([sys.executable, str(SCRIPT), "--config", str(config), "--results", str(output),
                            "--baseline", "base", "--rounds", str(rounds), "--timeout", str(timeout),
                            "--workdir", str(self.root)], capture_output=True, text=True, timeout=10)
        return p, output

    def test_valid_paired_runs_are_balanced_and_do_not_leak_url(self):
        p, output = self.run_gate()
        self.assertEqual(p.returncode, 0)
        doc = json.loads(output.read_text())
        self.assertEqual(len(doc["runs"]), 4)
        self.assertTrue(all(r["valid"] for r in doc["runs"]))
        self.assertEqual(doc["summary"]["example"]["candidate"]["paired_rounds"], 2)
        for tool in ("base", "candidate"):
            self.assertEqual({r["position"] for r in doc["runs"] if r["tool"] == tool}, {1, 2})
        self.assertNotIn("PRIVATE_SECRET", output.read_text() + p.stdout + p.stderr)
        self.assertFalse(any(p.name.startswith("rdm-gate-") for p in self.root.iterdir()))

    def test_wrong_hash_stops_and_persists_invalid_trial(self):
        self.cfg["targets"][0]["sha256"] = "0" * 64
        p, output = self.run_gate()
        self.assertEqual(p.returncode, 1)
        doc = json.loads(output.read_text())
        self.assertEqual(len(doc["runs"]), 1)
        self.assertFalse(doc["runs"][0]["valid"])
        self.assertIsNone(doc["runs"][0]["throughput_MiB_s"])
        self.assertNotIn("PRIVATE_SECRET", output.read_text() + p.stdout + p.stderr)

    def test_timeout_stops_and_is_recorded(self):
        self.fake.write_text("import time\ntime.sleep(5)\n")
        p, output = self.run_gate(timeout=0.1)
        self.assertEqual(p.returncode, 1)
        self.assertTrue(json.loads(output.read_text())["runs"][0]["timed_out"])

    def test_rejects_sensitive_or_duplicate_labels(self):
        self.cfg["tools"][0]["label"] = "https://example.test/?secret=bad"
        p, output = self.run_gate()
        self.assertEqual(p.returncode, 2)
        self.assertFalse(output.exists())
        self.assertNotIn("secret=bad", p.stdout + p.stderr)

    def test_existing_results_are_not_overwritten(self):
        output = self.root / "results.json"
        output.write_text("old")
        p, _ = self.run_gate()
        self.assertEqual(p.returncode, 2)
        self.assertEqual(output.read_text(), "old")


if __name__ == "__main__":
    unittest.main()
