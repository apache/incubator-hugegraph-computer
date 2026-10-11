# Repository notes

- `computer/`: Java 17 / Maven 3.6.3+; `vermeer/`: Go 1.23+.
- Keep Computer, Commons, Client and Loader versions tied to `${revision}`.
- Maven resolves Central first, then Apache staging; no stage profile or CI settings switch is needed.

## Computer checks

- Run Maven from `computer/`; test profiles are `unit-test` and `integrate-test`.
- Both suites use external services. Reproduce setup from `.github/workflows/computer-ci.yml` and `computer-dist/src/assembly/travis/`.
- CI tests Server `1.7.0` and `latest`; HDFS uses the Apache Hadoop 3.5.0 Docker image.
- Java processes embedding Computer need `--add-opens=java.base/java.io=ALL-UNNAMED` and `--add-opens=java.base/java.lang=ALL-UNNAMED`.
  The launcher and Surefire already supply them.
- K8s CRD classes are generated; build `computer-k8s-operator` before dependent modules and avoid editing generated sources.
- After dependency changes, regenerate `computer-dist/scripts/dependency/known-dependencies.txt` with its sibling script and update `computer-dist/release-docs/`.

## Vermeer

- First setup: `make init`; UI changes require `make generate-assets`.
- See `vermeer/AGENTS.md` for module-specific guidance.

## Formatting

- Code and fenced snippets: at most 120 characters; Java Checkstyle enforces this.
- `.editorconfig` provides editor defaults. Markdown uses a 160-character visual guide without forced wrapping.
- Preserve third-party LICENSE/NOTICE text; line-width rules do not apply to it.
