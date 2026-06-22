# Copilot Instructions

## Commit Message Format

Please follow the **Conventional Commits** specification for all commit messages:

```
<type>[optional scope]: <description>

[optional body]

[optional footer(s)]
```

### Types
- **feat**: A new feature
- **fix**: A bug fix
- **docs**: Documentation only changes
- **style**: Changes that do not affect the meaning of the code (white-space, formatting, etc)
- **refactor**: A code change that neither fixes a bug nor adds a feature
- **perf**: A code change that improves performance
- **test**: Adding missing tests or correcting existing tests
- **build**: Changes that affect the build system or external dependencies
- **ci**: Changes to CI configuration files and scripts
- **chore**: Other changes that don't modify src or test files

### Scopes (when applicable)
- **ingest**: Changes to the dispatcher, envelope/block types, or handler registry in `pkg/ingest/ingest.go`
- **detection**: Changes to the detection block handler or its actions in `pkg/ingest/detection.go`
- **marker**: Changes to the marker block handler or its action in `pkg/ingest/marker.go`

### Examples

**Good commit messages:**
```
feat(detection): promote face-redaction tracks to regions

fix(marker): stamp identity from target before validation

refactor(ingest): fail fast on disallowed sources before side effects

test(detection): cover rejected-box normalisation

chore: bump models dependency to latest release
```

## Code Style Guidelines

### Go Code
- Follow standard Go conventions (`gofmt`, `go vet`).
- Keep the package **infra-free**: depend only on `github.com/uug-ai/models` and the sink interfaces declared here.
- Add a new block kind as a new handler plus its ordered, idempotent actions — never grow a `switch` in the dispatcher.
- Sinks (`DetectionStore`, `MarkerStore`, `RegionPromoter`) are interfaces; concrete persistence lives in the calling application.

## Branch Naming
Use descriptive branch names with prefixes such as `feat/`, `fix/`, `refactor/`, or `chore/`.
