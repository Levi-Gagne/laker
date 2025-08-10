<!-- README.md (Layker) -->

<!-- ===== Top Banner (DO NOT CHANGE) ===== -->
<div align="center" style="margin-bottom: 18px;">
  <span style="font-size: 44px; line-height: 1; vertical-align: middle;">🐟</span>
  <span style="font-size: 44px; font-weight: bold; letter-spacing: 1.5px; color: #2186C4; vertical-align: middle;">Layker</span>
  <span style="font-size: 44px; line-height: 1; vertical-align: middle;">🐟</span>
  <br>
  <span style="font-size: 16px; color: #444; font-family: monospace; letter-spacing: 0.5px;">
    <b>L</b>akehouse‑<b>A</b>ligned <b>Y</b>AML <b>K</b>it for <b>E</b>ngineering <b>R</b>ules
  </span>
</div>

<hr style="border: 0; border-top: 2px solid #2186C4; margin: 8px 0 18px 0;">

<div style="font-size: 15px; color: #444;">
  Declarative <b>table metadata control</b> for Databricks & Spark.<br>
  Layker turns a YAML spec into <b>safe, validated DDL</b> with a built‑in <b>audit log</b>.<br>
  If nothing needs to change, Layker exits cleanly. If something must change, you’ll see it first.
</div>

<br>

<!-- ===== What is Layker ===== -->
<h2 style="color:#2186C4; font-weight:800; margin:16px 0 6px 0;">What is Layker?</h2>

<div style="font-size: 15px; color: #333;">
  Layker is a Python package for managing <b>table DDL, metadata, and auditing</b> with a single YAML file as the source of truth.
</div>

<table style="border-collapse: collapse; margin: 10px 0; width:100%; font-size:14px;">
  <tr>
    <td style="border:1px solid #e6f0f7; padding:10px; width:160px; background:#f6fbff;"><b>Declarative</b></td>
    <td style="border:1px solid #e6f0f7; padding:10px;">Author schemas, tags, constraints, and properties in YAML.</td>
  </tr>
  <tr>
    <td style="border:1px solid #e6f0f7; padding:10px; background:#f6fbff;"><b>Diff‑first</b></td>
    <td style="border:1px solid #e6f0f7; padding:10px;">Layker computes a diff against the live table; “no diff” = no work.</td>
  </tr>
  <tr>
    <td style="border:1px solid #e6f0f7; padding:10px; background:#f6fbff;"><b>Safe evolution</b></td>
    <td style="border:1px solid #e6f0f7; padding:10px;">Add/rename/drop column intents are validated and gated by required Delta properties.</td>
  </tr>
  <tr>
    <td style="border:1px solid #e6f0f7; padding:10px; background:#f6fbff;"><b>Auditable</b></td>
    <td style="border:1px solid #e6f0f7; padding:10px;">Every applied change is logged with <code>before/after</code> snapshots and a concise <code>differences</code> dictionary.</td>
  </tr>
  <tr>
    <td style="border:1px solid #e6f0f7; padding:10px; background:#f6fbff;"><b>Serverless‑friendly</b></td>
    <td style="border:1px solid #e6f0f7; padding:10px;">Gracefully skips <code>REFRESH TABLE</code> on serverless (prints a warning).</td>
  </tr>
</table>

<br>

<!-- ===== Installation ===== -->
<h2 style="color:#3B8C57; font-weight:800; margin:16px 0 6px 0;">Installation</h2>

<div style="border-left: 4px solid #3B8C57; background:#f3fbf6; padding:10px 12px; margin: 8px 0; color:#1f5134;">
  <b>Tip:</b> On Databricks, PySpark is already available. Layker won’t force-install it if your environment provides Spark.
</div>

**Stable**
```bash
pip install layker
```

**Latest (main)**
```bash
pip install "git+https://github.com/Levi-Gagne/layker.git"
```

<sub>Python 3.8+ and Spark 3.3+ recommended.</sub>

<br>

<!-- ===== Quickstart ===== -->
<h2 style="color:#2186C4; font-weight:800; margin:16px 0 6px 0;">Quickstart</h2>

<h4 style="margin:8px 0;">1) Author a YAML spec</h4>
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

<h4 style="margin:8px 0;">2) Sync from Python</h4>

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

<h4 style="margin:8px 0;">3) Or via CLI</h4>

```bash
python -m layker src/layker/resources/example.yaml prd false all true
```

<div style="border-left: 4px solid #2186C4; background:#f6fbff; padding:10px 12px; margin: 8px 0; color:#1a4d70;">
  When <code>audit_log_table=True</code>, Layker uses the packaged default:
  <code>layker/resources/layker_audit.yaml</code>. You can also pass a custom YAML path.
  Either way, the <b>YAML defines the audit table’s location</b>.
</div>

<br>

<!-- ===== How it works ===== -->
<h2 style="color:#3B8C57; font-weight:800; margin:16px 0 6px 0;">How it works (at a glance)</h2>

<ol>
  <li><b>Validate YAML</b> → fast fail with exact reasons, or proceed.</li>
  <li><b>Snapshot live table</b> (if it exists).</li>
  <li><b>Compute differences</b> between YAML snapshot and table snapshot.
    <div style="margin-top:4px; color:#555;">
      If <b>no changes</b> (diff contains only <code>full_table_name</code>), Layker exits with success and <b>no audit row</b>.
    </div>
  </li>
  <li><b>Validate differences</b> (schema‑evolution preflight):
    <ul>
      <li>Detects <code>add</code>/<code>rename</code>/<code>drop</code> column intents.</li>
      <li>Requires Delta properties:
        <code>delta.columnMapping.mode = name</code>,
        <code>delta.minReaderVersion = 2</code>,
        <code>delta.minWriterVersion = 5</code>.</li>
      <li>On missing requirements, prints details and exits.</li>
    </ul>
  </li>
  <li><b>Apply changes</b> (create/alter) using generated SQL.</li>
  <li><b>Audit</b> (only when changes were applied and auditing is enabled):
    writes a row with <code>before_value</code> (JSON), <code>differences</code> (JSON), <code>after_value</code> (JSON), <code>change_category</code>, and <code>change_key</code>.</li>
</ol>

<br>

<!-- ===== Audit log model ===== -->
<h2 style="color:#2186C4; font-weight:800; margin:16px 0 6px 0;">Audit log model</h2>

<div style="font-size: 15px; color: #333;">
The default audit YAML (<code>layker/resources/layker_audit.yaml</code>) defines these columns (in order):
</div>

<ul>
  <li><b>change_id</b> – UUID per row</li>
  <li><b>run_id</b> – optional job/run identifier</li>
  <li><b>env</b> – environment/catalog prefix</li>
  <li><b>yaml_path</b> – the source YAML path that initiated the change</li>
  <li><b>fqn</b> – fully qualified table name</li>
  <li><b>change_category</b> – <code>create</code> or <code>update</code></li>
  <li><b>change_key</b> – readable sequence per table (e.g., <code>create-1</code>, <code>create-1~update-2</code>)</li>
  <li><b>before_value</b> – JSON snapshot before change</li>
  <li><b>differences</b> – JSON diff dict that was applied</li>
  <li><b>after_value</b> – JSON snapshot after change</li>
  <li><b>notes</b> – optional free text</li>
  <li><b>created_at / created_by / updated_at / updated_by</b></li>
</ul>

<div style="border-left: 4px solid #e6b800; background:#fffaf0; padding:10px 12px; margin: 8px 0; color:#6a5800;">
  <b>Uniqueness expectation:</b> <code>(fqn, change_key)</code> is effectively unique over time.
</div>

<br>

<!-- ===== Modes ===== -->
<h2 style="color:#3B8C57; font-weight:800; margin:16px 0 6px 0;">Modes & parameters</h2>

<ul>
  <li><b>mode</b>: <code>validate</code> | <code>diff</code> | <code>apply</code> | <code>all</code>
    <ul>
      <li><b>validate</b>: only YAML validation (exits on success)</li>
      <li><b>diff</b>: prints proposed changes and exits</li>
      <li><b>apply</b>: applies changes only</li>
      <li><b>all</b>: validate → diff → apply → audit</li>
    </ul>
  </li>
  <li><b>audit_log_table</b>:
    <ul>
      <li><code>False</code> – disable auditing</li>
      <li><code>True</code> – use default <code>layker/resources/layker_audit.yaml</code></li>
      <li><code>str</code> – path to a custom audit YAML</li>
    </ul>
  </li>
  <li><b>No‑op safety</b>: if there are <b>no changes</b>, Layker exits early and skips audit.</li>
</ul>

<br>

<!-- ===== Serverless notes ===== -->
<h2 style="color:#2186C4; font-weight:800; margin:16px 0 6px 0;">Notes on serverless</h2>

<div style="border-left: 4px solid #2186C4; background:#f6fbff; padding:10px 12px; margin: 8px 0; color:#1a4d70;">
  Databricks serverless does <b>not</b> support <code>REFRESH TABLE</code>.
  Layker detects this and prints a warning; the rest of the flow continues.
</div>

<br>

<!-- ===== Repo layout ===== -->
<h2 style="color:#3B8C57; font-weight:800; margin:16px 0 6px 0;">Repository layout (typical)</h2>

```
src/
  layker/
    __init__.py
    __main__.py
    main.py
    differences.py
    loader.py
    logger.py
    snapshot_yaml.py
    snapshot_table.py
    resources/
      layker_audit.yaml
    utils/
      color.py
      printer.py
      spark.py
      timer.py
      paths.py
      table.py
    validators/
      params.py
      differences.py
```

<br>

<!-- ===== Troubleshooting ===== -->
<h2 style="color:#2186C4; font-weight:800; margin:16px 0 6px 0;">Troubleshooting</h2>

<ul>
  <li><b>Spark Connect / serverless</b>: Layker avoids schema inference issues by using explicit schemas when writing the audit row.</li>
  <li><b>Single quotes in comments</b>: Layker sanitizes YAML comments to avoid SQL quoting errors.</li>
  <li><b>No changes but I still see output</b>: A diff containing only <code>full_table_name</code> means no change; Layker exits early and writes no audit row.</li>
</ul>

<br>

<!-- ===== Contrib & License ===== -->
<h2 style="color:#3B8C57; font-weight:800; margin:16px 0 6px 0;">Contributing & License</h2>

PRs and issues welcome.<br>
License: see <code>LICENSE</code> in the repo.

<div align="center" style="margin-top: 18px;">
  <span style="font-size: 18px; color: #2186C4; font-weight: bold;">Built for engineers, by engineers.</span><br>
  <span style="font-size: 18px;">🐟&nbsp;LAYKER&nbsp;🐟</span>
</div>
