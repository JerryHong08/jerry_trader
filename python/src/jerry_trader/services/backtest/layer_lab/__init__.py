"""Layer Lab -- Boolean layer pipeline for premarket MOMO candidate filtration.

This is the dedicated workspace for the layer-based strategy research.
The pipeline applies sequential Boolean gates (LiquidityGate -> MomentumGate -> ...)
to filter premarket candidates, with right-tail evaluation (MOMO retention, PeakDelta).

Modules:
  - pipeline: BooleanLayer, CandidateContext, LayerPipeline, layer gates
  - lab: ExitLab -- exit timing research, strategy simulation, factor snapshots
  - analysis: _ExitLabAnalysis mixin -- MFE, sweep, tradability scoring
  - ignition: ignition detection helpers (volume expansion, range breakout, etc.)
  - types: ExitResult, ExitStrategyReport dataclasses
"""
