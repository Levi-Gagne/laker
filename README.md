<!-- README.md (Layker) -->

<div align="center" style="margin-bottom: 18px;">
  <span style="font-size: 44px; line-height: 1; vertical-align: middle;">🐟</span>
  <span style="font-size: 44px; font-weight: bold; letter-spacing: 1.5px; color: #2186C4; vertical-align: middle;">Layker</span>
  <span style="font-size: 44px; line-height: 1; vertical-align: middle;">🐟</span>
  <br>
  <span style="font-size: 16px; color: #444; font-family: monospace; letter-spacing: 0.5px;">
    <b>L</b>akehouse‑<b>A</b>ligned <b>Y</b>AML <b>K</b>it for <b>E</b>ngineering <b>R</b>ules
  </span>
</div>

---

<div style="font-size: 16px; line-height: 1.55;">
  Declarative <b>table metadata control</b> for Databricks &amp; Spark.<br>
  Layker turns a YAML spec into <b>safe, validated DDL</b> with a built‑in <b>audit log</b>.
  If nothing needs to change, Layker exits cleanly. If something must change, you’ll see it first.
</div>

<br/>

<details>
  <summary><b>Quick Navigation</b></summary>

- <a href="#what-is-layker">What is Layker?</a>
- <a href="#installation">Installation</a>
- <a href="#quickstart">Quickstart</a>
- <a href="#how-it-works-at-a-glance">How it works</a>
- <a href="#audit-log-model">Audit log model</a>
- <a href="#modes--parameters">Modes &amp; parameters</a>
- <a href="#serverless--classic-environments">Serverless &amp; classic</a>
- <a href="#repository-layout">Repository layout</a>
- <a href="#troubleshooting">Troubleshooting</a>
- <a href="#contributing--license">Contributing &amp; License</a>
</details>

---

## <span style="color:#2186C4;">What is Layker?</span>

Layker is a Python package for managing <b>table DDL, metadata, and auditing</b> with a single YAML file as the source of truth.

<div>
<b style="color:#3B8C57;">Highlights</b>
<ul>
  <li><b>Declarative</b> – author schemas, tags, constraints, and properties in YAML.</li>
  <li><b>Diff‑first</b> – Layker computes a diff against the live table; “no diff” = no work.</li>
  <li><b>Safe evolution</b> – add/rename/drop column intents are detected and gated by required Delta properties.</li>
  <li><b>Auditable</b> – every applied change is logged with <b>before/after</b> snapshots and a concise <b>differences</b> dictionary.</li>
  <li><b>Works in serverless or classic clusters</b> – avoids unsupported operations automatically.</li>
</ul>
</div>

---

## <span style="color:#2186C4;">Installation</span>

Stable:
```bash
pip install layker
```

Latest (main):
```bash
pip install "git+https://github.com/Levi-Gagne/layker.git"
```

Python 3.8+ and Spark 3.3+ are recommended. If you already have PySpark on the cluster, Layker will use it.

---

## <span id="quickstart" style="color:#2186C4;">Quickstart</span>

### 1) Author a YAML spec
Minimal example (save as <code>src/layker/resources/example.yaml</code>):
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

### 2) Sync from Python
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

### 3) Or via CLI
```bash
python -m layker src/layker/resources/example.yaml prd false all true
```

> When <code>audit_log_table=True</code>, Layker uses the packaged default:
> <code>layker/resources/layker_audit.yaml</code>.<br/>
> You can also pass a custom YAML path. Either way, the <b>YAML defines the audit table’s location</b>.

---

## <span style="color:#2186C4;">How it works (at a glance)</span>

1. <b>Validate YAML</b> → fast fail with exact reasons, or proceed.
2. <b>Snapshot live table</b> (if it exists).
3. <b>Compute differences</b> between YAML snapshot and table snapshot.
   - If <b>no changes</b> (i.e., the diff contains only <code>full_table_name</code>), <b>exit</b> with a success message and <b>no audit row</b> is written.
4. <b>Validate differences</b> (schema‑evolution preflight):
   - Detects <b>add/rename/drop</b> column intents.
   - Requires Delta properties for evolution:
     - <code>delta.columnMapping.mode = name</code>
     - <code>delta.minReaderVersion = 2</code>
     - <code>delta.minWriterVersion = 5</code>
   - On missing requirements, prints details and exits.
5. <b>Apply changes</b> (create/alter) using generated SQL.
6. <b>Audit</b> (only if changes were applied and auditing is enabled):
   - Writes a row containing:
     - <code>before_value</code> (JSON), <code>differences</code> (JSON), <code>after_value</code> (JSON)
     - <code>change_category</code> (<code>create</code> or <code>update</code>)
     - <code>change_key</code> (human‑readable sequence per table)
     - <code>env</code>, <code>yaml_path</code>, <code>fqn</code>, timestamps, actor, etc.

---

## <span style="color:#2186C4;">Audit log model</span>

The default audit YAML (<code>layker/resources/layker_audit.yaml</code>) defines these columns (in order):

- <b>change_id</b> – UUID per row
- <b>run_id</b> – optional job/run identifier
- <b>env</b> – environment/catalog prefix
- <b>yaml_path</b> – the source YAML path that initiated the change
- <b>fqn</b> – fully qualified table name
- <b>change_category</b> – <code>create</code> or <code>update</code> (based on whether a “before” snapshot was present)
- <b>change_key</b> – readable sequence per table:
  - First ever create: <code>create-1</code>
  - Subsequent updates on that lineage: <code>create-1~update-1</code>, <code>create-1~update-2</code>, …
  - If the table is later dropped &amp; re‑created: the next lineage becomes <code>create-2</code>, etc.
- <b>before_value</b> – JSON snapshot before change (may be null on first create)
- <b>differences</b> – JSON diff dict that was applied
- <b>after_value</b> – JSON snapshot after change
- <b>notes</b> – optional free text
- <b>created_at / created_by / updated_at / updated_by</b>

Uniqueness expectation: <code>(fqn, change_key)</code> is effectively unique over time.

---

## <span style="color:#2186C4;">Modes &amp; parameters</span>

- <b>mode</b>: <code>validate</code> | <code>diff</code> | <code>apply</code> | <code>all</code>
  - <code>validate</code>: only YAML validation (exits on success)
  - <code>diff</code>: prints proposed changes and exits
  - <code>apply</code>: applies changes only
  - <code>all</code>: validate → diff → apply → audit
- <b>audit_log_table</b>:
  - <code>False</code> – disable auditing
  - <code>True</code> – use default <code>layker/resources/layker_audit.yaml</code>
  - <code>str</code> – path to a custom audit YAML (the YAML governs the destination table)
- <b>No‑op safety</b>: if there are <b>no changes</b>, Layker exits early and <b>skips audit</b>.

---

## <span id="serverless--classic-environments" style="color:#2186C4;">Serverless &amp; classic environments</span>

Layker is compatible with Databricks <b>Serverless</b> and <b>classic</b> clusters. If an operation isn’t supported on serverless, Layker automatically avoids it and continues with the rest of the flow.

---

## <span id="repository-layout" style="color:#2186C4;">Repository layout</span>

For the full tree, see <a href="docs/tree.txt">docs/tree.txt</a>.
<details>
  <summary><b>Show condensed layout</b></summary>

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

---

## <span style="color:#2186C4;">Troubleshooting</span>

- <b>Spark Connect / serverless</b>: Layker avoids schema inference issues by using explicit schemas when writing the audit row.
- <b>Single quotes in comments</b>: Layker sanitizes YAML comments to avoid SQL quoting errors.
- <b>No changes but I still see output</b>: A diff containing only <code>full_table_name</code> means <b>no change</b>; Layker exits early with a success message and writes no audit row.

---

## <span style="color:#2186C4;">Contributing &amp; License</span>

PRs and issues welcome.<br/>
License: see <a href="LICENSE">LICENSE</a> in the repo.
</div>

<div align="center" style="margin-top: 18px;">
  <span style="font-size: 18px; color: #2186C4; font-weight: bold;">Built for engineers, by engineers.</span><br>
  <span style="font-size: 18px;">🐟&nbsp;LAYKER&nbsp;🐟</span>
</div>
