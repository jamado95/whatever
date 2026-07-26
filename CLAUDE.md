# Project Overview

A systematic trading engine with a complete backtesting pipeline. The goal of this project is to be both a research platform and live trading engine for multiple asset types such as FX, crypto and stocks.

Core Engine Language: golang
Preferred Research / Backtesting Language: python

To understand the architecture and strucutre of this project alwasy refer to `docs/Architecture.md` before engaging with any topic.

`docs/Commands.md` is the reference for every runnable command — build, run, checks, data downloads, visualisation and backtesting tooling. Refer to it instead of inferring commands from the Makefile, and keep it current when commands are added, changed or removed.


## Workflow

Every feature progresses through three stages:
 1. Discussion
 2. Implementation Plan
 3. Implementation + Testing

Each work stream has its own discussion document under `docs/discussions` to capture the exploration around a particular topic. Each document presents a summary of the topic under discussion, goals, constraints, trade-offs and current thinking. These documents are living records that can be iterated at any point when appropriate. Always inform when these docs are updated.

Critically, updates to these docs should only be done when explicitly prompted to do so!

Chats are where discussion is elaborated, the discussion docs are meant as briefs and summaries of ongoing chat discussions. They should not be full copies of the ongoing chat discussions. They are meant as state checkpoints from where indepth chat discussions can resume.

When a discussion reaches consensus, an associated implementation plan is created under `docs/plans`. Implementation plans are authoritative for engineering work. Claude should not implement directly from discussion documents. Implementation should always follow an approved implementation plan.

If implementation uncovers missing information, ambiguities, design issues or incompatibilities with existing architecture, pause implementation update the discussion and implementation plan for further clatification.

Multiple work streams may proceed in parallel, provided they concern independent topics.

Claude must:
1. Read the discussion doc before contributing to that topic. Create it for new discussions.
2. Record assumptions, decisions, open questions and next steps.
3. Summarize obsolete discussion rather than appending indefinitely.
4. Remove rejected ideas if they're not useful for ongoing discussion or historical context.
4. Cross-reference related discussions when appropriate.
5. Remind the user about updating the doc, as  discussion evolves sufficiently

Discussion docs are md filees following the format:
- Topic: context and objective of discussion
- Current Understanding: current state of reasoning around the topic. Very brief of summary of discussion.
- Decisions: brief summary of decisions made, with max two-lines of reasoning as to why
- Solution: current solution around the topic being discussed with supporting reasoning for it.
    - Keep immutable references to all executed implementation plans originated from the discussion.
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