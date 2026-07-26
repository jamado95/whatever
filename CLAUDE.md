# Project Overview

A systematic trading engine with a complete backtesting pipeline. The goal of this project is to be both a research platform and live trading engine for multiple asset types such as FX, crypto and stocks.

Core Engine Language: golang
Preferred Research / Backtesting Language: python


## Workflow

Every feature progresses through three stages:
 1. Discussion
 2. Implementation Plan
 3. Implementation + Testing

Each work stream has its own discussion document under `docs/discussions` to capture the exploration around a particular topic. Each document presents a summary of the topic under discussion, goals, constraints, trade-offs and current thinking. 

When a discussion reaches consensus, an associated implementation plan is created under `docs/plans`. Implementation plans are authoritative for engineering work. Claude should not implement directly from discussion documents. Implementation should always follow an approved implementation plan.

If implementation uncovers missing information, ambiguities, design issues or incompatibilities with existing architecture, pause implementation update the discussion and implementation plan for further clatification.

Multiple work streams may proceed in parallel, provided they concern independent topics.

Claude must:
1. Read the discussion doc before contributing to that topic. Create it for new discussions.
2. Update it as understanding evolves.
3. Record assumptions, decisions, open questions and next steps.
4. Keep the doc concise by summarizing obsolete discussion rather than appending indefinitely.
    4.1 Keep rejected ideas only if they're useful for historical context. Else, remove them.
5. Cross-reference related discussions when appropriate.

Discussion docs are md filees following the format:
- Topic: context and objective of discussion
- Current Understanding: current state of reasoning around the topic. Summary of discussion.
- Decisions: brief summary of decisions made, with max two-lines of reasoning as to why
- Solution: solutions considered with reasoning for them.
- Open Quesions
- WIP: current state of the work on this topic
- Resources: related discussion docs and/or other resources used to inform discussion

### Documentation

Always keep documentation, such as Architecture.md, up to date after changes to the code. Minor refactors or bug fixes that do not impact future work or discussions do not need to be documented.


## Testing

Run existing tests after changes to the code. Run any other project related tooling like compilers and linters if applicable.

Add tests for new behaviours. Prefer unit tests.

Briefly explain failing tests before changing them.


## Communication

Skip the pleasentries. Keep responses concise and focused on the topic, unless asked for detail.

Highlight alternatives when appropriate, for example in research or exploratory related discussions.

If uncertain, ask.

Put forward any ambiguities, or uncertainties in your responses. Do no present guesses as facts, always be clear about any unknows, assumptions and details that might require further exploration in your replies.