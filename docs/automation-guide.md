<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Automation & Code Quality Guide

This guide details the repository automations and code quality tools configured for **HugeGraph Computer** and **Vermeer** as recommended in Automation Analysis [#320](https://github.com/apache/hugegraph-computer/issues/320).

---

## Code Quality & Formatting Automations

### 1. Code Style & License Checks
- **Checkstyle (`maven-checkstyle-plugin`):** Enforces Java coding style guidelines defined in `checkstyle.xml`.
- **Apache RAT (`apache-rat-plugin`):** Verifies Apache license headers across all project source files.

```bash
# Run Checkstyle validation
mvn checkstyle:check

# Run Apache RAT license validation
mvn apache-rat:check
```

### 2. Test Coverage (`jacoco-maven-plugin`)
JaCoCo tracks unit and integration test coverage during build execution.

```bash
# Run unit tests and generate JaCoCo coverage report
mvn test -P unit-test

# Inspect generated report at:
# target/site/jacoco/jacoco.xml
```

---

## CI/CD Workflows

| Workflow | Path | Trigger | Description |
|----------|------|---------|-------------|
| **Commit Check** | `.github/workflows/commit-check.yml` | Pull Request | Validates PR titles against Conventional Commits formatting rules. |
| **Computer CI** | `.github/workflows/computer-ci.yml` | Push / PR | Compiles, runs RAT, HDFS, K8s, and Java unit/integration tests. |
| **Vermeer CI** | `.github/workflows/vermeer-ci.yml` | Push / PR | Builds Vermeer Go binary, checks UI assets, and tests Docker builds. |
| **Release Notes** | `.github/workflows/release-notes.yml` | Tag Push (`*.*.*`) | Automatically drafts GitHub release notes from git history. |

---

## PR Title Guidelines

Pull requests must follow the Conventional Commits specification:

`<type>(<scope>): <short summary>`

- **Allowed Types:** `feat`, `fix`, `docs`, `style`, `refactor`, `perf`, `test`, `chore`, `ci`, `build`
- **Examples:**
  - `feat(computer): support new edge format in 1.7`
  - `fix(vermeer): handle timeout during worker registration`
  - `ci(automation): add pr title validation and release workflows`
