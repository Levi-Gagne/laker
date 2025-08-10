
<!-- README.md (Layker) -->

<div align="center" style="margin-bottom: 18px;">
  <span style="font-size: 44px; line-height: 1; vertical-align: middle;">🐟</span>
  <span style="font-size: 44px; font-weight: bold; letter-spacing: 1.5px; color: #2186C4; vertical-align: middle;">Layker</span>
  <span style="font-size: 44px; line-height: 1; vertical-align: middle;">🐟</span>
  <br>
  <span style="font-size: 16px; color: #ccc; font-family: monospace; letter-spacing: 0.5px;">
    <b>L</b>akehouse‑<b>A</b>ligned <b>Y</b>AML <b>K</b>it for <b>E</b>ngineering <b>R</b>ules
  </span>
</div>

<!-- Dark wrapper (works on GitHub Pages and most Markdown renderers that allow inline styles) -->
<div style="background:#0f172a; color:#e5e7eb; padding:22px 22px 10px; border-radius:10px; box-shadow:0 2px 10px rgba(0,0,0,.25);">

<!-- Right-side floating TOC (degrades gracefully when styles are stripped) -->
<div style="float:right; width:260px; margin:6px 0 16px 16px; padding:12px; background:#0b1222; border:1px solid #1f2937; border-radius:8px;">
  <div style="font-weight:bold; color:#60a5fa; margin-bottom:8px;">Quick Nav</div>
  <ol style="margin:0; padding-left:16px; line-height:1.45;">
    <li><a href="#what-is-layker" style="color:#93c5fd;">What is Layker?</a></li>
    <li><a href="#core-idea" style="color:#93c5fd;">Core idea</a></li>
    <li><a href="#installation" style="color:#93c5fd;">Installation</a></li>
    <li><a href="#quickstart" style="color:#93c5fd;">Quickstart</a></li>
    <li><a href="#how-it-works" style="color:#93c5fd;">How it works</a></li>
    <li><a href="#audit-log-model" style="color:#93c5fd;">Audit log model</a></li>
    <li><a href="#repository-layout" style="color:#93c5fd;">Repository layout</a></li>
    <li><a href="#troubleshooting" style="color:#93c5fd;">Troubleshooting</a></li>
    <li><a href="#license--links" style="color:#93c5fd;">License & links</a></li>
  </ol>
</div>

<p style="margin-top:0; color:#e5e7eb;">
  <span style="color:#38bdf8; font-weight:600;">Declarative table metadata control</span> for Databricks & Spark.
  Layker turns a YAML spec into <b>safe, validated DDL</b> with a built‑in <b>audit log</b>.
  If nothing needs to change, Layker exits cleanly. If something must change, you’ll see it first.
</p>

---

<h2 id="what-is-layker" style="color:#60a5fa;">What is Layker?</h2>

<p>
Layker is a Python package for managing <b>table DDL, metadata, and auditing</b> with a single YAML file as the source of truth.
It is designed to be <i>Spark/Delta‑native</i> and to fit cleanly into existing ETL workflows.
</p>

<ul>
  <li><b>Declarative:</b> Author schemas, tags, constraints, properties, owners, and comments in YAML.</li>
  <li><b>Diff‑first:</b> Layker computes a diff against the live table; “no diff” = no work.</li>
  <li><b>Safe evolution:</b> add/rename/drop column intents are detected and gated by the required Delta properties.</li>
  <li><b>Auditable:</b> every applied change is logged with <i>before/after</i> snapshots and a concise <i>differences</i> dictionary.</li>
  <li><b>Works anywhere you have Spark:</b> serverless or classic clusters—no special privileges required.</li>
  <li><b>AI‑friendly:</b> readable YAML puts all table knowledge in one place for search and LLMs.</li>
</ul>

---

<h2 id="core-idea" style="color:#60a5fa;">Core idea</h2>

<p>
<strong>Infrastructure‑as‑Code for Lakehouse table metadata.</strong>
Instead of scattering SQL across notebooks and jobs, you keep a single YAML per table.
Layker validates, diffs, applies what changed, and writes a structured audit record—automatically.
</p>

---

<h2 id="installation" style="color:#60a5fa;">Installation</h2>

<p>Stable:</p>
<pre style="background:#0b1222; padding:12px; border-radius:8px;"><code>$ pip install layker</code></pre>

<p>Latest (main):</p>
<pre style="background:#0b1222; padding:12px; border-radius:8px;"><code>$ pip install "git+https://github.com/Levi-Gagne/layker.git"</code></pre>

<p style="color:#9ca3af;">Python 3.8+ and Spark 3.3+ are recommended. On Databricks, Spark is preinstalled—Layker will use your existing runtime.</p>

---

<h2 id="quickstart" style="color:#60a5fa;">Quickstart</h2>

<p><b>1) Author a YAML spec</b> (save as <code>src/layker/resources/example.yaml</code>):</p>

```yaml
catalog: dq_dev
schema: lmg_sandbox
table: layker_test

columns:
  1:
    name: id
    datatype: bigint
    nullable: false
    active: true
  2:
    name: name
    datatype: string
    nullable: true
    active: true

table_comment: Demo table managed by Layker
table_properties:
  delta.columnMapping.mode: "name"
  delta.minReaderVersion: "2"
  delta.minWriterVersion: "5"

primary_key: [id]
tags:
  domain: demo
  owner: team-data
```

<p><b>2) Sync from Python</b></p>

```python
from pyspark.sql import SparkSession
from layker.main import run_table_load

spark = SparkSession.builder.appName("layker").getOrCreate()

run_table_load(
    yaml_path="src/layker/resources/example.yaml",
    env="prd",
    dry_run=False,
    mode="all",                 # validate | diff | apply | all
    audit_log_table=True        # True=default audit YAML, False=disable, or str path to an audit YAML
)
```

<p><b>3) Or via CLI</b></p>

```bash
python -m layker src/layker/resources/example.yaml prd false all true
```

<p style="color:#9ca3af;">
When <code>audit_log_table=True</code>, Layker uses the packaged default:
<code>layker/resources/layker_audit.yaml</code>.
You can also pass a custom YAML path. Either way, the <b>YAML defines the audit table’s location</b>.
</p>

---

<h2 id="how-it-works" style="color:#60a5fa;">How it works</h2>

<ol>
  <li><b>Validate YAML</b> → fast fail with exact reasons, or proceed.</li>
  <li><b>Snapshot live table</b> (if it exists).</li>
  <li><b>Compute differences</b> between YAML snapshot and table snapshot.
    <ul>
      <li>If <b>no changes</b> (only <code>full_table_name</code> present), Layker exits with a success message—<i>no audit row is written</i>.</li>
    </ul>
  </li>
  <li><b>Validate differences</b> (schema‑evolution preflight).
    <ul>
      <li>Detects <i>add/rename/drop</i> column intents.</li>
      <li>Requires Delta props:
        <code>delta.columnMapping.mode=name</code>,
        <code>delta.minReaderVersion=2</code>,
        <code>delta.minWriterVersion=5</code>.</li>
    </ul>
  </li>
  <li><b>Apply changes</b> (create/alter) using generated SQL.</li>
  <li><b>Audit</b> (only if changes were applied and auditing enabled): writes before/diff/after with actor and timestamps.</li>
</ol>

---

<h2 id="audit-log-model" style="color:#60a5fa;">Audit log model</h2>

<p>The default audit YAML (<code>layker/resources/layker_audit.yaml</code>) defines:</p>

<ul>
  <li><b>change_id</b> (UUID), <b>run_id</b> (optional), <b>env</b>, <b>yaml_path</b>, <b>fqn</b></li>
  <li><b>change_category</b> (create|update) &amp; <b>change_key</b> (e.g., <code>create-1</code>, <code>create-1~update-2</code>)</li>
  <li><b>before_value</b> (JSON), <b>differences</b> (JSON), <b>after_value</b> (JSON)</li>
  <li><b>notes</b>, <b>created_at</b>/<b>created_by</b>, <b>updated_at</b>/<b>updated_by</b></li>
</ul>

<p><i>Uniqueness expectation:</i> <code>(fqn, change_key)</code> is effectively unique over time.</p>

---

<h2 id="repository-layout" style="color:#60a5fa;">Repository layout</h2>

<details>
  <summary style="cursor:pointer; color:#93c5fd;">Show tree (click to expand)</summary>

```
layker/
├── .github/
│   └── workflows/
│       └── workflow.yaml
│
├── archive/
│   ├── main.py
│   ├── sanitizer.py
│   ├── snapshot_yaml.py
│   ├── steps_audit.py
│   ├── steps_differences.py
│   ├── steps_loader.py
│   ├── validate.py
│   ├── validators_evolution.py
│   └── yaml.py
│
├── docs/
│   ├── audit.md
│   ├── differences.txt
│   ├── FAQ
│   ├── FLOW
│   ├── future_enhancements.txt
│   ├── snapshot.txt
│   └── tree.txt
│
├── src/
│   ├── layker/
│   │   ├── resources/
│   │   │   ├── config_driven_table_example.yaml
│   │   │   ├── example.yaml
│   │   │   ├── layker_audit.yaml
│   │   │   └── layker_test.yaml
│   │   │
│   │   ├── utils/
│   │   │   ├── __init__.py
│   │   │   ├── color.py
│   │   │   ├── dry_run.py
│   │   │   ├── paths.py
│   │   │   ├── printer.py
│   │   │   ├── spark.py
│   │   │   ├── table.py
│   │   │   ├── timer.py
│   │   │   └── yaml_table_dump.py
│   │   │
│   │   ├── validators/
│   │   │   ├── __init__.py
│   │   │   ├── differences.py
│   │   │   └── params.py
│   │   │
│   │   ├── __about__.py
│   │   ├── __init__.py
│   │   ├── __main__.py
│   │   ├── differences.py
│   │   ├── loader.py
│   │   ├── logger.py
│   │   ├── main.py
│   │   ├── snapshot_table.py
│   │   └── snapshot_yaml.py
│   │
│   ├── dev_testing.ipynb
│   └── test_layker.ipynb
│
├── tests/
│   ├── __init__.py
│   ├── test_loader.py
│   └── test_main.py
│
├── .gitignore
├── LICENSE
├── MANIFEST.in
├── pyproject.toml
├── README.md
└── requirements.txt
```
</details>

<p>
See the full, generated tree in <a href="./docs/tree.txt">docs/tree.txt</a>.<br/>
A detailed flow-of-control doc is at <a href="./docs/FLOW">docs/FLOW</a> and FAQs at <a href="./docs/FAQ">docs/FAQ</a>.
</p>

---

<h2 id="troubleshooting" style="color:#60a5fa;">Troubleshooting</h2>

<ul>
  <li><b>Serverless or classic:</b> Layker runs on Databricks Serverless and standard clusters. It avoids operations not supported by your runtime and proceeds safely.</li>
  <li><b>Spark Connect inference:</b> The audit writer uses explicit schemas to avoid type‑inference issues.</li>
  <li><b>Quoting:</b> YAML comments are sanitized to prevent single‑quote SQL errors.</li>
  <li><b>No changes but I still see output:</b> A diff containing only <code>full_table_name</code> means no change; Layker exits early and does not write an audit row.</li>
</ul>

---

<h2 id="license--links" style="color:#60a5fa;">License & links</h2>

<ul>
  <li>License: <a href="./LICENSE" style="color:#93c5fd;">LICENSE</a></li>
  <li>PyPI: <a href="https://pypi.org/project/layker/" style="color:#93c5fd;">pypi.org/project/layker</a></li>
  <li>Source: <a href="https://github.com/Levi-Gagne/layker" style="color:#93c5fd;">github.com/Levi-Gagne/layker</a></li>
  <li>Docs index: <a href="./README.md" style="color:#93c5fd;">README.md</a></li>
</ul>

</div>
