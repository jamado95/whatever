Robust Backtesting Framework

A Layer-Aware, Statistical and Temporal Validation Process

⸻

## 1. Objective

This document defines a structured, falsification-driven framework for validating trading strategies. It aligns with a clean architectural separation:
	•	Processing → Feature construction
	•	Signal → Alpha / trade intent
	•	Execution → P&L realization

The goal is to progressively eliminate alternative explanations for performance:
	1.	Implementation error
	2.	Parameter luck
	3.	Statistical randomness
	4.	Regime dependence
	5.	Combined overfitting

Only strategies that survive all stages qualify as credible alpha candidates.

⸻

## Stage 0 — Deterministic Baseline Backtest

### Purpose

Validate engine correctness before any statistical inference.

### Procedure
	•	Single parameter set
	•	Full historical dataset
	•	No optimization

Evaluate
	•	Trade logic correctness
	•	Slippage/fee realism
	•	Position accounting
	•	Clean layer separation

Failure Signals
	•	Performance highly sensitive to execution assumptions
	•	Inconsistent trade accounting
	•	Hidden leakage across layers

⸻

## Stage 1 — Parameter Exploration (In-Sample Only)

### Purpose

Assess structural robustness of the strategy.

### Procedure
	•	Grid or randomized parameter search
	•	Evaluate performance surface

Evaluate
	•	Smoothness of performance surface
	•	Existence of stable parameter regions
	•	Sensitivity to ±5–10% parameter perturbations
	•	Trade concentration (edge driven by few trades?)
	•	Turnover sensitivity

Reject If
	•	Sharp isolated peaks
	•	Performance collapses under small parameter shifts
	•	Edge dominated by rare events

This stage tests parameter robustness.

⸻

## Stage 2 — Null Hypothesis Design

### Purpose

Formally define what the strategy claims to exploit.

Step 2A — Identify Structural Dependency

Strategy Type	Claimed Exploit	Null Must Remove
Moving averages	Serial correlation	Autocorrelation
Mean reversion	Short-term reversal structure	Conditional dependence
Candle patterns	Local conditional structure	Pattern-specific dependencies
Volatility breakout	Volatility clustering	Volatility persistence
Gap strategy	Overnight regime structure	Regime continuity

Step 2B — Choose Null Strength
	•	Permissive null → Preserves distribution, removes specific dependency
	•	Weak null → Removes signal mechanism only
	•	Strong null → Removes most serial structure
	•	Destructive null → Removes nearly all temporal structure

Test against multiple strengths.

This stage defines the counterfactual world.

⸻

## Stage 3 — Monte Carlo Under Null

### Purpose

Test statistical significance.

### Procedure
	•	Generate 1,000+ synthetic datasets under null
	•	Run full backtest pipeline

Evaluate
	•	Sharpe distribution
	•	CAGR distribution
	•	Max drawdown distribution
	•	Empirical p-value
	•	Quantile rank of observed Sharpe

Reject If
	•	Observed performance lies within 95% null distribution
	•	Edge disappears under weak null

This stage tests statistical validity.

⸻

## Stage 4 — Walk-Forward Optimization

### Purpose

Test temporal stability.

### Procedure
	•	Rolling or anchored window
	•	Optimize on past
	•	Trade next segment OOS

Evaluate
	•	IS vs OOS performance decay
	•	Parameter stability across windows
	•	Rolling Sharpe consistency
	•	Drawdown behavior over time

Reject If
	•	Large IS → OOS degradation
	•	Parameter instability
	•	Edge concentrated in one regime

This stage tests temporal robustness.

⸻

## Stage 5 — Monte Carlo Walk-Forward

### Purpose

Test combined statistical and temporal robustness.

### Procedure

For each null dataset:
	1.	Perform full walk-forward optimization
	2.	Record OOS performance

Evaluate
	•	Distribution of OOS Sharpe under null
	•	Compare real OOS Sharpe to null WF distribution
	•	Probability of achieving observed robustness randomly

Interpretation

If real walk-forward performance exceeds null WF distribution with high confidence, robustness is unlikely to be random.

This is the strongest anti-overfitting test.

⸻

## Decision Criteria

A strategy progresses only if:
	1.	Smooth parameter surface
	2.	Statistically significant under weak null
	3.	Stable walk-forward performance
	4.	Monte Carlo walk-forward rejects null

⸻

## Summary Flow
	1.	Validate implementation
	2.	Test parameter robustness
	3.	Define structural null
	4.	Run Monte Carlo significance test
	5.	Validate temporal robustness
	6.	Combine MC + walk-forward

Each stage eliminates a different failure mode.

⸻

## Final Principle

Backtesting is not about maximizing Sharpe.
It is about systematically rejecting alternative explanations for observed performance.

Only what survives falsification deserves capital.