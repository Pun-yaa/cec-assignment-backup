"""Verify document completeness and evidence without starting application services."""
from __future__ import annotations
import datetime
import hashlib
import importlib.metadata
import json
import re
from pathlib import Path

import pymupdf
from markdown_it import MarkdownIt
from reportlab.pdfbase.ttfonts import TTFont
import reportlab

HERE=Path(__file__).resolve().parent
ROOT=HERE.parents[1]
md=MarkdownIt("commonmark").enable("table")
errors=[]
link_count=0
heading_count=0
word_count=0
files=sorted(HERE.glob("0[1-7]-*.md"))
assert len(files)==7
oral=(HERE/"04-oral-exam-handbook.md").read_text()
questions=re.findall(r"^### (\d+) ",oral,re.M)
assert questions==[str(i) for i in range(1,51)]
def normalize(s):
    return re.sub(r"\s+"," ",s.replace("→","->")).strip()
font=TTFont("AuditStudy",str(Path(reportlab.__file__).parent/"fonts/Vera.ttf"))
for p in [HERE/"README.md",*files]:
    content=p.read_text()
    word_count+=len(content.split())
    tokens=md.parse(content)
    pdfpath=HERE/"pdf"/(p.stem+".pdf" if p.name!="README.md" else "group-15-exam-preparation.pdf")
    pdf=pymupdf.open(pdfpath)
    text=normalize(" ".join(page.get_text() for page in pdf))
    for i,t in enumerate(tokens):
        if t.type=="heading_open":
            plain=tokens[i+1].content
            heading_count+=1
            if normalize(plain) not in text:errors.append({"missing_pdf_heading":plain,"file":p.name})
        if t.type=="inline":
            for child in t.children or []:
                if child.type in {"link_open","image"}:
                    target=child.attrGet("href") if child.type=="link_open" else child.attrGet("src")
                    if not re.match(r"^[a-zA-Z]+:",target) and not target.startswith("#"):
                        q=(p.parent/target.split("#",1)[0]).resolve()
                        link_count+=1
                        if not q.exists():errors.append({"broken_link":target,"file":p.name})
                if child.type=="text":
                    for c in child.content.replace("→","->"):
                        if ord(c)>127 and ord(c) not in font.face.charWidths:
                            errors.append({"unsupported_glyph":c,"file":p.name})
    if "\ufffd" in text:errors.append({"replacement_character":p.name})
    pdf.close()
pdf_details=[]
for p in sorted((HERE/"pdf").glob("*.pdf")):
    pdf=pymupdf.open(p)
    for n,page in enumerate(pdf,1):
        for word in page.get_text("words"):
            x0,y0,x1,y1,*_=word
            if x0<25 or x1>page.rect.width-25 or y0<8 or y1>page.rect.height-8:
                errors.append({"bounds":word[:5],"pdf":p.name,"page":n})
    pdf_details.append({"file":str(p.relative_to(HERE)),"pages":len(pdf),
                        "bookmarks":len(pdf.get_toc()),"sha256":hashlib.sha256(p.read_bytes()).hexdigest()})
    pdf.close()

baseline=json.loads((ROOT/"IMPLEMENTATION-MANIFEST.json").read_text())
changed=[]
for item in baseline["files"]:
    if item["status"]=="D":continue
    p=ROOT/item["path"]
    if not p.exists() or hashlib.sha256(p.read_bytes()).hexdigest()!=item["snapshot_sha256"]:
        changed.append(item["path"])
assert changed==[],changed

before=json.loads((ROOT/"benchmarks/2026-10-09/course-stress-initial.json").read_text())
after=json.loads((ROOT/"benchmarks/2026-10-09/course-stress.json").read_text())
assert before["notifications"]["over_10_seconds"]==3
assert after["notifications"]["required"]==after["notifications"]["received"]==610
assert after["notifications"]["max_latency_seconds"]<10
assert after["histories"]["history_endpoint_checks"]==202
assert after["concurrent_experiments"]==100 and after["sensors"]==10
assert f'{after["achieved_http_rps"]:.3f}'=="97.310"
assert f'{100*(1-after["notifications"]["max_latency_seconds"]/before["notifications"]["max_latency_seconds"]):.2f}'=="47.36"
assert f'{100*(after["achieved_http_rps"]/before["achieved_http_rps"]-1):.2f}'=="12.05"

# Compare private values without displaying them.
body=b"\n".join(p.read_bytes() for p in HERE.rglob("*")
               if p.is_file() and p.suffix in {".md",".json",".txt",".py",".svg"})
checked=0
for folder in ("group15-credentials","group-15-credentials",".course-test"):
    directory=ROOT/folder
    if not directory.exists():continue
    for p in directory.rglob("*"):
        if not p.is_file() or p.stat().st_size>2_000_000:continue
        if p.name not in {"token","grafana","keystore-password","secret.env"} and p.suffix not in {".key",".pem"}:continue
        raw=p.read_bytes().strip()
        candidates=[raw] if len(raw)>=12 else []
        for line in raw.splitlines():
            if b"=" in line:
                key,value=line.split(b"=",1)
                if any(word in key.upper() for word in (b"PASSWORD",b"SECRET",b"TOKEN")):
                    value=value.strip().strip(b'"').strip(b"'")
                    if len(value)>=12:candidates.append(value)
            if b"PRIVATE KEY" in raw and len(line)>40 and not line.startswith(b"-----"):
                candidates.append(line)
        for value in candidates:
            checked+=1
            if value in body:errors.append({"private_credential_detected":True})
result={
    "passed":not errors,
    "checked_at_utc":datetime.datetime.now(datetime.timezone.utc).isoformat(),
    "editable_reports":7,"oral_questions":50,"local_links_checked":link_count,
    "headings_checked_against_pdf_text":heading_count,"markdown_word_count_including_index":word_count,
    "numeric_comparisons_checked_against_raw_json":True,
    "application_source_unchanged":True,
    "private_values_checked_without_output":checked,
    "pdfs":pdf_details,
    "documentation_dependencies":{n:importlib.metadata.version(n) for n in
                                 ("reportlab","pymupdf","markdown-it-py")},
    "errors":errors,
}
(HERE/"evidence/document-verification.json").write_text(json.dumps(result,indent=2)+"\n")
print(json.dumps(result,indent=2))
if errors:raise SystemExit(1)
