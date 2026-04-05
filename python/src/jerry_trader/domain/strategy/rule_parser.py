"""Strategy DSL Rule Parser

Parses YAML rule definitions into domain Rule objects.
Includes factor reference validation against FactorRegistry.

Usage:
    from jerry_trader.domain.strategy.rule_parser import parse_rule_file, load_rules

    rules = load_rules("config/rules/")
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Callable

import yaml

from jerry_trader.domain.strategy.rule import (
    ComparisonOp,
    Rule,
    RuleValidationResult,
    Trigger,
    TriggerType,
    ValidationIssue,
)

logger = logging.getLogger(__name__)

# Type alias for factor registry checker: takes (factor_id, timeframe) → bool
FactorChecker = Callable[[str, str], bool]


# ─────────────────────────────────────────────────────────────────────────────
# Parsing
# ─────────────────────────────────────────────────────────────────────────────


def parse_rule(data: dict[str, Any]) -> Rule:
    """Parse a rule from a dict (e.g., loaded from YAML).

    Args:
        data: Raw dict with rule fields.

    Returns:
        Validated Rule object.

    Raises:
        ValueError: If the rule data is invalid.
    """
    try:
        return Rule.model_validate(data)
    except Exception as e:
        rule_id = data.get("id", "<unknown>")
        raise ValueError(f"Invalid rule '{rule_id}': {e}") from e


def parse_rule_file(path: Path | str) -> Rule:
    """Parse a rule from a YAML file.

    Args:
        path: Path to YAML rule file.

    Returns:
        Validated Rule object.

    Raises:
        FileNotFoundError: If file doesn't exist.
        ValueError: If the rule is invalid.
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Rule file not found: {path}")

    with open(path, "r") as f:
        data = yaml.safe_load(f)

    if not isinstance(data, dict):
        raise ValueError(
            f"Rule file must contain a YAML dict, got {type(data).__name__}"
        )

    return parse_rule(data)


def load_rules_from_dir(dir_path: Path | str) -> list[Rule]:
    """Load all rule YAML files from a directory.

    Skips files that fail to parse with a warning.

    Args:
        dir_path: Directory containing YAML rule files.

    Returns:
        List of successfully parsed Rule objects.
    """
    dir_path = Path(dir_path)
    if not dir_path.is_dir():
        logger.warning(f"Rules directory not found: {dir_path}")
        return []

    rules: list[Rule] = []
    for path in sorted(dir_path.glob("*.yaml")):
        try:
            rule = parse_rule_file(path)
            rules.append(rule)
            logger.debug(f"Loaded rule: {rule.id} from {path.name}")
        except Exception as e:
            logger.warning(f"Failed to parse rule from {path.name}: {e}")

    logger.info(f"Loaded {len(rules)} rules from {dir_path}")
    return rules


# ─────────────────────────────────────────────────────────────────────────────
# Factor Reference Validation
# ─────────────────────────────────────────────────────────────────────────────


def validate_rule_factors(
    rule: Rule,
    checker: FactorChecker,
) -> RuleValidationResult:
    """Validate that all factor references in a rule exist in the registry.

    Args:
        rule: The rule to validate.
        checker: Function that takes (factor_id, timeframe) and returns True
                 if the factor is available for that timeframe.

    Returns:
        RuleValidationResult with any issues found.
    """
    issues: list[ValidationIssue] = []

    for i, condition in enumerate(rule.trigger.conditions):
        path = f"trigger.conditions[{i}].factor"

        # Check factor exists
        if not checker(condition.factor, condition.timeframe):
            # Try without timeframe constraint to give better message
            if not checker(condition.factor, ""):
                issues.append(
                    ValidationIssue(
                        path=path,
                        message=f"Unknown factor: '{condition.factor}'",
                        severity="error",
                    )
                )
            else:
                issues.append(
                    ValidationIssue(
                        path=path,
                        message=(
                            f"Factor '{condition.factor}' is not available "
                            f"for timeframe '{condition.timeframe}'"
                        ),
                        severity="error",
                    )
                )

        # Check cross target exists (if applicable)
        if condition.target and condition.op in (
            ComparisonOp.CROSS_ABOVE,
            ComparisonOp.CROSS_BELOW,
        ):
            if not checker(condition.target, condition.timeframe):
                issues.append(
                    ValidationIssue(
                        path=f"trigger.conditions[{i}].target",
                        message=f"Unknown cross target: '{condition.target}'",
                        severity="error",
                    )
                )

    return RuleValidationResult(
        rule_id=rule.id,
        valid=len(issues) == 0,
        issues=issues,
    )


def validate_rules(
    rules: list[Rule],
    checker: FactorChecker,
) -> list[RuleValidationResult]:
    """Validate factor references for multiple rules.

    Args:
        rules: List of rules to validate.
        checker: Factor existence checker.

    Returns:
        List of validation results.
    """
    return [validate_rule_factors(rule, checker) for rule in rules]


def make_registry_checker(registry) -> FactorChecker:
    """Create a FactorChecker from a FactorRegistry instance.

    Args:
        registry: FactorRegistry with get_spec() method.

    Returns:
        FactorChecker function.
    """

    def checker(factor_id: str, timeframe: str) -> bool:
        spec = registry.get_spec(factor_id)
        if spec is None:
            return False
        # If no timeframe constraint, just check factor exists
        if not timeframe:
            return True
        # Check if factor supports this timeframe
        return timeframe in spec.timeframes

    return checker
