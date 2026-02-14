# MachineLearning – Stage 1: Context v0 minimal ML scaffold
#
# Pipeline:  raw → dataset → train → evaluate → inference
#
# Modules
# -------
# mock_data  – synthetic sample generator (hidden-rule labels)
# dataset    – ContextV0 schema + feature-matrix builder
# model      – LightGBM wrapper (create / save / load / predict)
# train      – training loop & end-to-end pipeline runner
# evaluate   – regression metrics & single-sample inference demo
