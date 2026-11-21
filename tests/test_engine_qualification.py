import importlib.util
import json
from pathlib import Path
import re
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parent.parent
COMPARE = ROOT / "scripts/compare-engine-qualification.py"


def report(version, outcome=None):
    return {
        "engine_version": version,
        "queries": [{
            "id": "case",
            "sql": "SELECT 1",
            "outcome": outcome or {"kind": "success", "columns": ["value"], "column_types": ["Int32"], "rows": [["1"]]},
            "elapsed_ms": 10,
            "profile": {"status": "captured"},
        }],
    }


class EngineQualificationComparisonTest(unittest.TestCase):
    def compare(self, baseline, candidate, expectations):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name, value in [("baseline.json", baseline), ("candidate.json", candidate), ("expectations.json", expectations)]:
                (root / name).write_text(json.dumps(value))
            result = subprocess.run([
                "python3", str(COMPARE), "--baseline", str(root / "baseline.json"),
                "--candidate", str(root / "candidate.json"), "--expectations", str(root / "expectations.json"),
                "--output", str(root / "comparison.json"),
            ], text=True, capture_output=True)
            output = root / "comparison.json"
            return result, json.loads(output.read_text()) if output.exists() else None

    def test_unchanged_result_passes_and_reports_relative_timing(self):
        baseline = report("v1.5.4")
        candidate = report("v2.0.1")
        candidate["queries"][0]["elapsed_ms"] = 15
        result, comparison = self.compare(baseline, candidate, {})
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(comparison["counts"], {"unchanged": 1, "accepted": 0, "fixed": 0, "blocking": 0})
        self.assertEqual(comparison["queries"][0]["performance"]["candidate_over_baseline"], 1.5)

    def test_classifies_accepted_and_fixed_differences(self):
        changed = {"kind": "error", "status": 422, "failure_kind": "sql_error", "retry_safe": False, "message": "changed"}
        for classification in ["accepted", "fixed"]:
            with self.subTest(classification=classification):
                result, comparison = self.compare(report("v1.5.4"), report("v2.0.1", changed), {classification: ["case"]})
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(comparison["counts"][classification], 1)

    def test_unclassified_or_missing_results_block(self):
        changed = {"kind": "success", "columns": ["value"], "column_types": ["Int64"], "rows": [["1"]]}
        for candidate in [report("v2.0.1", changed), {"engine_version": "v2.0.1", "queries": []}]:
            with self.subTest(candidate=candidate):
                result, comparison = self.compare(report("v1.5.4"), candidate, {})
                self.assertEqual(result.returncode, 1)
                self.assertEqual(comparison["counts"]["blocking"], 1)

    def test_same_engine_needs_an_explicit_tooling_fixture_override(self):
        result, comparison = self.compare(report("v1.5.4"), report("v1.5.4"), {})
        self.assertEqual(result.returncode, 1)
        self.assertTrue(comparison["version_mismatch"])
        result, comparison = self.compare(report("v1.5.4"), report("v1.5.4"), {"allow_same_version": True})
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_unknown_expectation_is_rejected(self):
        result, _ = self.compare(report("v1.5.4"), report("v2.0.1"), {"accepted": ["unknown"]})
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("unknown baseline cases", result.stderr)


class EngineQualificationLaneTest(unittest.TestCase):
    def test_workers_and_gateways_share_isolated_result_prefixes(self):
        spec = importlib.util.spec_from_file_location("qualify_engine", ROOT / "scripts/qualify-engine.py")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        with tempfile.TemporaryDirectory() as directory:
            paths = module.write_override(Path(directory) / "override.yaml", "engine-qualification/test")
            text = (Path(directory) / "override.yaml").read_text()
        self.assertEqual(paths["lake"], "s3://duckfleet/engine-qualification/test/lake/")
        self.assertEqual(paths["results"], "s3://duckfleet/engine-qualification/test/results/")
        for service in ["worker-small", "server"]:
            section = re.split(r"\n  (?=\S)", text.split(f"  {service}:\n", 1)[1], maxsplit=1)[0]
            self.assertIn(paths["lake"], section)
            self.assertIn(paths["results"], section)
        worker = re.split(r"\n  (?=\S)", text.split("  worker-small:\n", 1)[1], maxsplit=1)[0]
        self.assertIn("S3_DATA_PATH", worker)


if __name__ == "__main__":
    unittest.main()
