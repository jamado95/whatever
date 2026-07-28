# Project Overview

A systematic trading engine with a complete backtesting pipeline. The project serves both as a research platform and a live trading engine for multiple asset classes, including FX, crypto, and equities.

- **Core engine:** Go
- **Research / backtesting:** Python

## Authority

Use the following sources in order of precedence:

1. `docs/Architecture.md` — project architecture, design, and engineering principles.
2. `docs/plans/*` — approved implementation plans.
3. `docs/discussions/*` — design discussions and current reasoning.
4. Source code.

Do not infer architecture. Always consult `docs/Architecture.md`.

Do not infer build, run, testing, or tooling commands. Use `docs/Commands.md`, and keep it updated whenever commands are added, removed, or changed.

## Workflow

Every feature follows this sequence:

> Discussion → Implementation Plan → Implementation & Testing

### Discussion

Each topic has a discussion document under `docs/discussions`.

Discussion documents:
- Capture design reasoning and decisions.
- Serve as concise state snapshots, not chat transcripts.
- Summarize rather than append indefinitely.
- Remove obsolete or rejected ideas unless they provide useful historical context.
- Cross-reference related discussions when appropriate.

Only update discussion documents when explicitly requested, or within the scope of an implementation plan

If implementation uncovers ambiguity, missing information, architectural conflicts, or design issues:
- Stop implementation.
- Explain the blocking issue clearly. 
- Request clarification before proceeding.


Before contributing to an existing topic:
1. Read its discussion document.
2. Record assumptions, decisions, open questions, and next steps.
3. Notify the user if the discussion document is updated.

Discussion documents follow this structure:

- Topic
- Current Understanding
- Decisions
- Solution
  - Include immutable references to implementation plans executed for the topic.
- Open Questions
- WIP
- Resources

### Implementation Plans

Implementation plans are stored under `docs/plans`.

- Plans are the authoritative source for engineering work.
- Never implement directly from discussion documents.
- Plans should include targeted updates to the associated discussion document, if applicable.
- After an approved plan is executed
  - Store it in `docs/plans`.
  - Reference it from the associated discussion document when appropriate.

## Documentation

Update documentation after architectural, workflow, or other changes that affect future development.

Minor refactors and bug fixes that do not impact future work do not require documentation updates.

Always notify the user when documentation is modified.

## Implementation & Testing

Implementation checklist:

- Follow the approved implementation plan.
- Prefer extending existing abstractions over introducing new patterns.
- Make the smallest change that satisfies the requirement.
- Avoid unrelated refactoring.
- Add tests for new behavior, preferably unit tests.
- Run relevant project tests.
- Run applicable compilers, linters, and project tooling.
- Briefly explain failing tests before modifying them.

## Communication

- Skip pleasantries.
- Be concise unless additional detail is requested.
- Present alternatives when appropriate.
- State assumptions and uncertainties explicitly.
- Never present guesses as facts.
- Ask for clarification when uncertain.