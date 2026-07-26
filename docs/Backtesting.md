BACKTESTING

⸻

Trading Strategy Development & Backtesting Process

This document describes the methodology used to design, validate, and stress-test trading strategies. The goal is to distinguish real, persistent market signals from patterns created by overfitting and data mining.

Optimization is unavoidable, but it always introduces bias. The process below is designed to measure and control that bias, not eliminate it.

⸻

1. In-Sample Excellence

Objective:
Verify that the strategy can extract meaningful performance on historical data under idealized assumptions.

What happens here:
	•	Define the strategy logic (signals, rules, parameters)
	•	Enforce causality:
	•	Signals use only information available at time t
	•	Returns are realized from t → t+1
	•	No same-bar signal/return overlap (no look-ahead bias)
	•	Compute baseline metrics (Sharpe, profit factor, drawdown, etc.)

What this step proves:
	•	The idea is not obviously broken
	•	There is at least some structure being exploited

What it does NOT prove:
	•	That the strategy is real
	•	That performance will persist out of sample

In-sample performance is a necessary but meaningless condition on its own.

⸻

2. In-Sample Permutation Test

Objective:
Test whether in-sample performance could be explained purely by overfitting noise.

Null hypothesis:

The strategy has no real edge and is fitting random structure.

Procedure:
	•	Destroy temporal structure in the data (e.g. permute or shuffle returns)
	•	Keep marginal statistics (distribution, volatility) intact
	•	Re-optimize the strategy on this noise data
	•	Repeat many times to build a distribution of “best possible” noise performance

Interpretation:
	•	If the strategy performs similarly on noise, it is overfit
	•	If real-data performance lies far in the tail of noise results, the signal may be real

Why this matters:
	•	Optimization always finds something
	•	This test answers: “Would optimization have worked just as well on pure randomness?”

⸻

3. Walk-Forward Test

Objective:
Test temporal stability and simulate real deployment.

Procedure:
	•	Split data into rolling windows:
	•	Train / optimize on past data
	•	Evaluate on unseen future data
	•	Repeat forward through time
	•	Aggregate out-of-sample results

What this tests:
	•	Parameter robustness
	•	Regime sensitivity
	•	Whether performance survives without access to future data

Key principle:

In live trading, parameters are chosen before outcomes are known.

This step enforces that constraint.

⸻

4. Walk-Forward Permutation Test

Objective:
Stress-test the entire research pipeline against false discovery.

Procedure:
	•	Apply the full walk-forward process to permuted (noise) data
	•	Measure the best performance achievable through:
		• Optimization
		• Walk-forward selection
		• Chance alone

Interpretation:
	•	If real walk-forward results are not statistically distinct from noise:
		•	The pipeline is discovering patterns that do not exist
		•	This is the strongest defense against research overfitting

⸻

Core Principles Tied to This Process
	•	Causality first
Signals must precede returns. Any same-bar dependency is invalid.
	•	Returns belong to decisions made before the price move
This is why return alignment (e.g. shifting) matters in backtests.
	•	Optimization reveals bias; it does not create truth
A good strategy works across ranges of parameters, not sharp peaks.
	•	Noise always fits
The question is whether the strategy fits better than noise.

⸻

Summary
	1.	In-sample excellence checks that the idea is viable
	2.	In-sample permutation checks that performance isn’t just noise
	3.	Walk-forward testing checks temporal robustness
	4.	Walk-forward permutation checks the entire process for false discovery

A strategy is credible only if it survives all four steps.

⸻