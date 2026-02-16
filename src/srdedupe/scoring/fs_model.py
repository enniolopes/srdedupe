"""Fellegi-Sunter model loader and scorer.

This module implements the Fellegi-Sunter probabilistic record linkage model,
including model configuration loading, weight calculation, and posterior
probability computation.
"""

import json
import math
from pathlib import Path
from typing import Any


def sigmoid(x: float) -> float:
    """Compute sigmoid function.

    Parameters
    ----------
    x : float
        Input value.

    Returns
    -------
    float
        Sigmoid of x (0.0-1.0).

    Notes
    -----
    sigmoid(x) = 1 / (1 + exp(-x))
    """
    if x >= 0:
        return 1.0 / (1.0 + math.exp(-x))
    # For numerical stability with large negative x
    exp_x = math.exp(x)
    return exp_x / (1.0 + exp_x)


def logit(p: float) -> float:
    """Compute logit function (inverse of sigmoid).

    Parameters
    ----------
    p : float
        Probability (0.0-1.0).

    Returns
    -------
    float
        Logit of p.

    Raises
    ------
    ValueError
        If p not in (0, 1).

    Notes
    -----
    logit(p) = log(p / (1 - p))
    """
    if p <= 0.0 or p >= 1.0:
        raise ValueError(f"Probability must be in (0, 1), got {p}")
    return math.log(p / (1.0 - p))


class FSModel:
    """Fellegi-Sunter model configuration and scorer.

    Attributes
    ----------
    name : str
        Model name.
    version : str
        Model version.
    round_decimals : int
        Decimal places for output rounding.
    """

    __slots__ = (
        "name",
        "version",
        "round_decimals",
        "lambda_prior",
        "_logit_prior",
        "_field_weights",
    )

    def __init__(self, config: dict[str, Any]) -> None:
        """Initialize FS model from configuration.

        Parameters
        ----------
        config : dict[str, Any]
            Model configuration dictionary.

        Raises
        ------
        KeyError
            If required configuration keys are missing.
        """
        self.name: str = config["model"]["name"]
        self.version: str = config["model"]["version"]
        self.lambda_prior: float = config["lambda_prior"]
        self._logit_prior: float = logit(self.lambda_prior)  # Cache
        self.round_decimals: int = config["round_decimals"]

        # Build weight lookup: {field: {level: weight}}
        self._field_weights: dict[str, dict[str, float]] = {}
        for field_name, field_config in config["fields"].items():
            self._field_weights[field_name] = {
                level["name"]: level["weight"] for level in field_config["levels"]
            }

    def get_weight(self, field: str, level: str) -> float:
        """Get weight for a field comparison level.

        Parameters
        ----------
        field : str
            Field name.
        level : str
            Comparison level.

        Returns
        -------
        float
            Log-likelihood ratio weight.

        Raises
        ------
        KeyError
            If field or level not found in model.
        """
        return self._field_weights[field][level]

    def compute_llr(self, field_levels: dict[str, str]) -> float:
        """Compute log-likelihood ratio for field comparisons.

        Parameters
        ----------
        field_levels : dict[str, str]
            Dictionary mapping field names to comparison levels.

        Returns
        -------
        float
            Total LLR score.

        Notes
        -----
        LLR = logit(lambda_prior) + sum(weights)
        """
        llr = self._logit_prior
        for field, level in field_levels.items():
            llr += self._field_weights[field][level]
        return llr

    def compute_p_match(self, llr: float) -> float:
        """Compute posterior match probability from LLR.

        Parameters
        ----------
        llr : float
            Log-likelihood ratio.

        Returns
        -------
        float
            Posterior probability of match (0.0-1.0).

        Notes
        -----
        p(match) = sigmoid(LLR)
        """
        return sigmoid(llr)

    def round_value(self, value: float) -> float:
        """Round value to model precision.

        Parameters
        ----------
        value : float
            Value to round.

        Returns
        -------
        float
            Rounded value.
        """
        return round(value, self.round_decimals)

    def get_top_contributions(
        self,
        field_comparisons: dict[str, tuple[str, float]],
        top_k: int = 5,
    ) -> tuple[dict[str, Any], ...]:
        """Get top contributing fields sorted by absolute weight.

        Parameters
        ----------
        field_comparisons : dict[str, tuple[str, float]]
            Dictionary mapping field names to (level, weight) tuples.
        top_k : int, optional
            Number of top contributions to return, by default 5.

        Returns
        -------
        tuple[dict[str, Any], ...]
            Top contributions sorted by absolute weight descending.
            Each entry has keys: 'field', 'level', 'weight'.
        """
        sorted_fields = sorted(
            field_comparisons.items(),
            key=lambda x: abs(x[1][1]),
            reverse=True,
        )
        return tuple(
            {"field": field, "level": level, "weight": self.round_value(weight)}
            for field, (level, weight) in sorted_fields[:top_k]
        )


def load_model(model_path: Path | str) -> FSModel:
    """Load Fellegi-Sunter model from JSON file.

    Parameters
    ----------
    model_path : Path | str
        Path to model JSON file.

    Returns
    -------
    FSModel
        Loaded model.

    Raises
    ------
    FileNotFoundError
        If model file not found.
    KeyError
        If model configuration is invalid.
    """
    path = Path(model_path)
    if not path.exists():
        raise FileNotFoundError(f"Model file not found: {path}")

    with path.open("r") as f:
        config = json.load(f)

    return FSModel(config)
