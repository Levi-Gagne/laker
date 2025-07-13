
<!-- README.md (Layker) -->

<div align="center" style="margin-bottom: 24px;">
  <span style="font-size: 48px; line-height: 1; vertical-align: middle;">🐟</span>
  <span style="font-size: 48px; font-weight: bold; letter-spacing: 2px; color: #2186C4; vertical-align: middle;">Layker</span>
  <span style="font-size: 48px; line-height: 1; vertical-align: middle;">🐟</span>
  <br>
  <span style="font-size: 18px; color: #444; font-family: monospace; letter-spacing: 1px;">
    <b>L</b>akehouse-<b>A</b>ligned <b>Y</b>AML <b>K</b>it for <b>E</b>ngineering <b>R</b>ules
  </span>
</div>

<hr style="border: 0; border-top: 1.5px solid #2186C4; margin-bottom: 18px;">

<div style="font-size: 20px; color: #3B8C57; font-weight: bold;">Declarative Table Metadata Control for Databricks & Spark</div>
<p style="color: #444;">
  Layker gives you a <b>YAML-driven, audit-friendly workflow</b> for evolving table schemas, properties, tags, and constraints—without the manual SQL, and without surprises.
  <br>
  <b>Everything is validated. Every change is logged.</b> Know exactly what’s changing, every time.
</p>

---

## <span style="color: #2186C4;">What Is Layker?</span>

Layker is a modern Python package for managing <b>table DDL, metadata, and audit logs</b> in Databricks (or any Spark 3+ platform), using just a YAML file as your single source of truth.

<table>
  <tr>
    <td><b>Declarative</b></td>
    <td>Just write your schema and metadata in YAML.</td>
  </tr>
  <tr>
    <td><b>Diff-First</b></td>
    <td>See exactly what will change before you touch production.</td>
  </tr>
  <tr>
    <td><b>Safe</b></td>
    <td>Schema evolution is validated, logged, and reversible. No silent data loss.</td>
  </tr>
  <tr>
    <td><b>Auditable</b></td>
    <td>Every change is tracked in a dedicated audit log table.</td>
  </tr>
</table>

---

## <span style="color: #3B8C57;">How It Works</span>

<ol>
  <li><b>Write your YAML spec</b> – see <code>src/layker/config/example.yaml</code>.</li>
  <li><b>Run Layker</b> (from Python or CLI) with your YAML and environment.</li>
  <li>
    <b>Layker validates</b> your YAML and logic.<br>
    <span style="color: #2186C4;">If invalid:</span> fails fast, tells you why.<br>
    <span style="color: #3B8C57;">If valid:</span> checks if table exists.
  </li>
  <li>
    <b>Table does not exist?</b><br>
    &nbsp;&nbsp;→ <span style="color: #2186C4;">Creates table exactly as described.</span>
  </li>
  <li>
    <b>Table exists?</b><br>
    &nbsp;&nbsp;→ Compares YAML spec to live table.<br>
    &nbsp;&nbsp;→ If no differences: <span style="color: #2186C4;">exits, nothing to do.</span><br>
    &nbsp;&nbsp;→ If differences: <span style="color: #3B8C57;">prints proposed changes, validates for schema evolution, applies updates.</span>
  </li>
  <li>
    <b>All actions are logged</b> to an audit table (see <code>changelog/load_table_log.yaml</code>).
  </li>
</ol>

---

## <span style="color: #2186C4;">Quickstart</span>

<b>1. Install</b>
```bash
pip install layker
```
or, for the latest dev build:
```bash
pip install git+https://github.com/Levi-Gagne/layker.git
```

<b>2. Write your YAML</b>

See <code>src/layker/config/example.yaml</code>:

```yaml
catalog: dq_dev
schema: my_schema
table: customer_dim
columns:
  1:
    name: id
    datatype: bigint
    nullable: false
  2:
    name: name
    datatype: string
    nullable: false
properties:
  comment: Customer master table
  table_properties:
    delta.enableChangeDataFeed: true
primary_key: [id]
tags:
  domain: core
```

<b>3. Sync your table (Python or CLI)</b>

Python:
```python
from layker.main import run_table_load

run_table_load(
    yaml_path="src/layker/config/example.yaml",
    env="prod",
    dry_run=False,
    mode="apply"
)
```

CLI:
```bash
python -m layker.main src/layker/config/example.yaml prod false apply
```
---

## <span style="color: #3B8C57;">Repo Structure</span>

<pre>
layker/
├── src/
│   └── layker/
│       ├── main.py
│       ├── yaml_reader.py
│       ├── validator.py
│       ├── sanitizer.py
│       ├── introspector.py
│       ├── differences.py
│       ├── differences_logger.py
│       ├── loader.py
│       ├── color.py
│       ├── dry_run.py
│       ├── retry.py
│       ├── config/
│       │   └── example.yaml
│       └── changelog/
│           ├── loader_logger.py
│           └── load_table_log.yaml
├── tests/
├── README.md
├── FLOW.txt
├── TREE.txt
</pre>

---

## <span style="color: #2186C4;">Core Modules</span>

<table>
  <tr>
    <td><b>main.py</b></td>
    <td>Entrypoint for CLI and import. Runs end-to-end logic.</td>
  </tr>
  <tr>
    <td><b>yaml_reader.py</b></td>
    <td>Loads and exposes all YAML values as attributes.</td>
  </tr>
  <tr>
    <td><b>validator.py</b></td>
    <td>Checks YAML validity, required fields, and business logic.</td>
  </tr>
  <tr>
    <td><b>sanitizer.py</b></td>
    <td>Normalizes YAML and table metadata for reliable diffing.</td>
  </tr>
  <tr>
    <td><b>introspector.py</b></td>
    <td>Pulls table metadata from Spark/Databricks.</td>
  </tr>
  <tr>
    <td><b>differences.py</b></td>
    <td>Computes difference between YAML spec and actual table.</td>
  </tr>
  <tr>
    <td><b>loader.py</b></td>
    <td>Applies table DDL (create/alter) based on diff.</td>
  </tr>
  <tr>
    <td><b>changelog/loader_logger.py</b></td>
    <td>Writes all changes to the audit table (config in <code>load_table_log.yaml</code>).</td>
  </tr>
</table>

---

## <span style="color: #3B8C57;">FAQ</span>

<b>What does Layker support?</b>
<ul>
  <li>YAML-driven creation and evolution of Spark/Delta tables (Databricks-ready)</li>
  <li>Automatic audit logging of all schema and property changes</li>
  <li>Safe, detailed validation and diffing before any change</li>
</ul>

<b>Can I use this outside Databricks?</b>
<ul>
  <li>Yes. It works with any Spark 3.x cluster, but audit logging and some table features are Delta-specific.</li>
</ul>

<b>What if my YAML or table is invalid?</b>
<ul>
  <li>Layker always fails fast, tells you exactly why, and never half-applies a change.</li>
</ul>

---

<div align="center" style="color: #2186C4; margin-top: 36px; font-weight: bold;">
  Built for engineers, by engineers. <br>
  <span style="font-size: 20px;">🐟&nbsp;LAYKER&nbsp;🐟</span>
</div>
