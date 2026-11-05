"""Audit the study pack and package public documents/source/evidence; no live mutations."""

from __future__ import annotations

import datetime
import hashlib
import importlib.metadata
import json
import re
import statistics
import zipfile
from pathlib import Path

import pymupdf
import reportlab
from markdown_it import MarkdownIt
from reportlab.pdfbase.ttfonts import TTFont

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
md = MarkdownIt("commonmark").enable("table")


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read_result(name, date="2026-11-05"):
    return json.loads((ROOT / "benchmarks" / date / (name + ".json")).read_text())


def local_links(path):
    for token in md.parse(path.read_text()):
        if token.type != "inline":
            continue
        for child in token.children or []:
            if child.type not in {"link_open", "image"}:
                continue
            target = child.attrGet("href" if child.type == "link_open" else "src")
            if not re.match(r"^[a-zA-Z]+:", target) and not target.startswith("#"):
                yield target, (path.parent / target.split("#", 1)[0]).resolve()


def normalize(text):
    return re.sub(r"\s+", " ", text.replace("→", "->")).strip()


def main():
    errors = []
    files = sorted(HERE.glob("0[1-9]-*.md"))
    assert len(files) == 9
    oral = (HERE / "04-oral-exam-handbook.md").read_text()
    assert re.findall(r"^### (\d+) ", oral, re.M) == [str(i) for i in range(1, 61)]
    manifest = json.loads((HERE / "evidence/source-manifest.json").read_text())
    for item in manifest["files"]:
        if sha(ROOT / item["path"]) != item["sha256"]:
            errors.append({"source_changed_since_report_start": item["path"]})
    november = json.loads((ROOT / "reports/code-improvements-2026-11-05/manifest.json").read_text())
    for item in november["files"]:
        if sha(ROOT / item["path"]) != item["after_sha256"]:
            errors.append({"november_change_snapshot_drift": item["path"]})

    native = read_result("course-variable-final")
    fixed = read_result("course-200-final")
    independent = read_result("variable-100-final")
    boundary = read_result("variable-final")
    assert native["passed"] and fixed["passed"] and independent["passed"]
    assert not boundary["passed"] and boundary["http_errors"] == 0
    assert native["notifications"]["required"] == native["notifications"]["received"] == 703
    assert fixed["notifications"]["required"] == fixed["notifications"]["received"] == 596
    assert all(r["notifications"]["at_or_over_10_seconds"] == 0 for r in (native, fixed))
    assert all(
        r["histories"]["history_endpoint_checks"] == 202 and not r["histories"]["failures"]
        for r in (native, fixed)
    )
    assert [p["target_rps"] for p in native["variable_http_validation"]["phases"]] == [
        148,
        182,
        181,
        114,
        106,
    ]
    assert all(p["passed"] for p in native["variable_http_validation"]["phases"])
    assert all(p["passed"] for p in independent["http_phases"])
    peak = boundary["http_phases"][1]
    assert (
        peak["expected_requests"] == 6000
        and peak["scheduled_requests"] == peak["successful_requests"] == 5526
    )
    assert peak["expected_requests"] - peak["scheduled_requests"] == 474
    assert not peak["passed"] and not boundary["http_phases"][2]["passed"]
    assert independent["notifications_expected"] == independent["notifications_received"] == 1626
    assert boundary["notifications_expected"] == boundary["notifications_received"] == 1626
    assert "92 passed in 13.12s" in (ROOT / "benchmarks/2026-11-05/tests-final.txt").read_text()
    readiness = read_result("readiness-final")
    assert readiness["tests"] == {"passed": 92, "integration": 14}
    assert read_result("demo-kafka-lag-final")["total_lag"] == 0
    assert read_result("recovery")["passed"]
    old_before = read_result("course-stress-initial", "2026-10-09")
    old_after = read_result("course-stress", "2026-10-09")
    assert (
        round(
            100
            * (
                1
                - old_after["notifications"]["max_latency_seconds"]
                / old_before["notifications"]["max_latency_seconds"]
            ),
            2,
        )
        == 47.36
    )
    assert (
        round(100 * (old_after["achieved_http_rps"] / old_before["achieved_http_rps"] - 1), 2)
        == 12.05
    )

    numeric = {
        "native_variable_max_seconds": native["notifications"]["max_latency_seconds"],
        "native_fixed_max_seconds": fixed["notifications"]["max_latency_seconds"],
        "native_fixed_achieved_rps": fixed["achieved_http_rps"],
        "native_fixed_margin_above_local_threshold_rps": fixed["achieved_http_rps"] - 190,
        "independent_pass_peak_achieved_rps": independent["http_phases"][1]["achieved_rps"],
        "independent_pass_max_notification_seconds": independent["notification_latency_seconds"][
            "max"
        ],
        "independent_failed_peak_achieved_rps": peak["achieved_rps"],
        "independent_failed_peak_omitted_offers": 474,
        "independent_failed_max_notification_seconds": boundary["notification_latency_seconds"][
            "max"
        ],
        "mean_sampled_cpu_percent": {},
        "metadata_read_deltas": {},
    }
    for name in ["variable-baseline", "variable-metadata-cache", "variable-final"]:
        rows = json.loads((ROOT / "benchmarks/2026-11-05" / f"{name}.telemetry.json").read_text())
        key = "to_database_pool_wait_seconds_count{operation=history_metadata}"
        numeric["metadata_read_deltas"][name] = rows[-1]["data_metrics"].get(key, 0) - rows[0][
            "data_metrics"
        ].get(key, 0)
        numeric["mean_sampled_cpu_percent"][name] = 100 * statistics.mean(
            r["host_cpu_utilization"] for r in rows if "host_cpu_utilization" in r
        )
    assert numeric["metadata_read_deltas"]["variable-baseline"] == 4546
    assert numeric["metadata_read_deltas"]["variable-metadata-cache"] == 2732
    assert round(numeric["mean_sampled_cpu_percent"]["variable-final"], 2) == 99.38
    submission = (HERE / "08-submission-report.md").read_text()
    for key in [
        "native_variable_max_seconds",
        "native_fixed_max_seconds",
        "native_fixed_achieved_rps",
        "independent_pass_peak_achieved_rps",
        "independent_failed_peak_achieved_rps",
        "independent_pass_max_notification_seconds",
        "independent_failed_max_notification_seconds",
    ]:
        assert f"{numeric[key]:.3f}" in submission, key
    (HERE / "evidence/derived-results.json").write_text(json.dumps(numeric, indent=2) + "\n")

    link_count = heading_count = 0
    font = TTFont("AuditStudy", str(Path(reportlab.__file__).parent / "fonts/Vera.ttf"))
    for path in [HERE / "README.md", *files]:
        pdfpath = (
            HERE
            / "pdf"
            / ("group-15-assessment.pdf" if path.name == "README.md" else path.stem + ".pdf")
        )
        document = pymupdf.open(pdfpath)
        text = normalize(" ".join(page.get_text() for page in document))
        tokens = md.parse(path.read_text())
        for i, token in enumerate(tokens):
            if token.type == "heading_open":
                heading_count += 1
                if normalize(tokens[i + 1].content) not in text:
                    errors.append({"missing_pdf_heading": tokens[i + 1].content, "file": path.name})
            if token.type == "inline":
                for child in token.children or []:
                    if child.type == "text":
                        for c in child.content.replace("→", "->"):
                            if ord(c) > 127 and ord(c) not in font.face.charWidths:
                                errors.append({"unsupported_glyph": c, "file": path.name})
        for target, resolved in local_links(path):
            link_count += 1
            if not resolved.exists():
                errors.append({"broken_link": target, "file": path.name})
        if "\ufffd" in text:
            errors.append({"replacement_character": path.name})
        document.close()
    pdfs = []
    pdf_texts = []
    for path in sorted((HERE / "pdf").glob("*.pdf")):
        document = pymupdf.open(path)
        for n, page in enumerate(document, 1):
            pdf_texts.append(page.get_text())
            for word in page.get_text("words"):
                x0, y0, x1, y1, *_ = word
                if x0 < 25 or x1 > page.rect.width - 25 or y0 < 8 or y1 > page.rect.height - 8:
                    errors.append({"text_outside_bounds": word[:5], "file": path.name, "page": n})
        pdfs.append(
            {
                "file": path.relative_to(HERE).as_posix(),
                "pages": len(document),
                "bookmarks": len(document.get_toc()),
                "sha256": sha(path),
            }
        )
        document.close()

    # Package explicit public sources and report/evidence dependencies, never runtime state.
    packed = {ROOT / item["path"] for item in manifest["files"]}
    for dirname in ["reports/final-exam", "reports/code-improvements-2026-11-05", "benchmarks"]:
        packed.update(
            p for p in (ROOT / dirname).rglob("*") if p.is_file() and "__pycache__" not in p.parts
        )
    packed.update(
        ROOT / name
        for name in [
            "README.md",
            "MONITORING.md",
            "STRESS-TEST-RESULTS.md",
            "IMPLEMENTATION-REPORT.md",
            "IMPLEMENTATION-CHANGES.patch",
            "IMPLEMENTATION-MANIFEST.json",
            "Group_Assignment_Rubric_INFOMCEC_2026-2027-Final.pdf",
            "reports/README.md",
        ]
    )
    packed.update(
        p
        for p in HERE.rglob("*")
        if p.is_file()
        and "__pycache__" not in p.parts
        and p.name
        not in {"group-15-assessment-pack.zip", "bundle-manifest.json", "verification.json"}
    )

    def public(path):
        relative = path.relative_to(ROOT)
        return not any(
            part.startswith(".env")
            or part
            in {
                "auth",
                ".git",
                ".venv",
                ".course-test",
                "target",
                "group15-credentials",
                "group-15-credentials",
                "__pycache__",
            }
            for part in relative.parts
        ) and path.suffix not in {".key", ".pem", ".pkcs12"}

    for path in packed:
        if not public(path):
            errors.append({"nonpublic_archive_path": str(path.relative_to(ROOT))})
    # Resolve links from the current pack; archived report links retain their historical context.
    for path in [HERE / "README.md", *files]:
        for _target, resolved in local_links(path):
            if resolved.is_file() and resolved.is_relative_to(ROOT) and public(resolved):
                packed.add(resolved)

    body = b"\n".join(
        p.read_bytes()
        for p in packed
        if p.suffix
        in {
            ".md",
            ".json",
            ".txt",
            ".py",
            ".yml",
            ".sql",
            ".sh",
            ".toml",
            ".patch",
            ".svg",
            ".html",
            ".lock",
        }
    )
    body += "\n".join(pdf_texts).encode()
    checked = 0
    for folder in ["group15-credentials", "group-15-credentials", ".course-test"]:
        directory = ROOT / folder
        if not directory.exists():
            continue
        for path in directory.rglob("*"):
            if not path.is_file() or path.stat().st_size > 2_000_000:
                continue
            if path.name not in {
                "token",
                "grafana",
                "keystore-password",
                "secret.env",
            } and path.suffix not in {".key", ".pem"}:
                continue
            raw = path.read_bytes().strip()
            candidates = [raw] if len(raw) >= 12 else []
            for line in raw.splitlines():
                if b"=" in line:
                    key, value = line.split(b"=", 1)
                    if any(word in key.upper() for word in (b"PASSWORD", b"SECRET", b"TOKEN")):
                        value = value.strip().strip(b'"').strip(b"'")
                        if len(value) >= 12:
                            candidates.append(value)
                if b"PRIVATE KEY" in raw and len(line) > 40 and not line.startswith(b"-----"):
                    candidates.append(line)
            for value in candidates:
                checked += 1
                if value in body:
                    errors.append({"private_value_found": True})

    result = {
        "passed": not errors,
        "checked_at_utc": datetime.datetime.now(datetime.UTC).isoformat(),
        "editable_reports": 9,
        "oral_questions": 60,
        "source_files": len(manifest["files"]),
        "local_links_checked": link_count,
        "headings_checked": heading_count,
        "markdown_words_excluding_code_appendix": sum(
            len(p.read_text().split()) for p in [HERE / "README.md", *files]
        ),
        "application_source_unchanged": not any(
            "source_changed_since_report_start" in e for e in errors
        ),
        "numeric_evidence_checked": True,
        "private_values_checked_without_display": checked,
        "pdfs": pdfs,
        "documentation_dependencies": {
            n: importlib.metadata.version(n) for n in ["reportlab", "pymupdf", "markdown-it-py"]
        },
        "errors": errors,
    }
    verification = HERE / "evidence/verification.json"
    verification.write_text(json.dumps(result, indent=2) + "\n")
    if errors:
        print(
            json.dumps(
                {"passed": False, "errors": errors[:15], "error_count": len(errors)}, indent=2
            )
        )
        raise SystemExit(1)
    packed.add(verification)
    bundle = {
        "created_at_utc": result["checked_at_utc"],
        "scope": "Public reports, code, rubric and saved evidence. Runtime credentials excluded. Deployment still requires the assignment checkout.",
        "files": [
            {"path": p.relative_to(ROOT).as_posix(), "sha256": sha(p)} for p in sorted(packed)
        ],
    }
    bundle_path = HERE / "bundle-manifest.json"
    bundle_path.write_text(json.dumps(bundle, indent=2) + "\n")
    archive = HERE / "group-15-assessment-pack.zip"
    with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as output:
        for path in sorted(packed | {bundle_path}):
            output.write(path, path.relative_to(ROOT).as_posix())
    with zipfile.ZipFile(archive) as saved:
        assert saved.testzip() is None
        for entry in bundle["files"]:
            assert hashlib.sha256(saved.read(entry["path"])).hexdigest() == entry["sha256"]
    print(
        json.dumps(
            {
                "passed": True,
                "reports": 9,
                "oral_questions": 60,
                "source_files": len(manifest["files"]),
                "pdfs": len(pdfs),
                "combined_pages": next(
                    p["pages"] for p in pdfs if p["file"].endswith("group-15-assessment.pdf")
                ),
                "archive_bytes": archive.stat().st_size,
                "local_links": link_count,
                "source_unchanged": result["application_source_unchanged"],
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
