"""
Strategy Parser - Load and parse strategy definitions from YAML/JSON

This module provides functions to:
- Parse strategy definitions from YAML or JSON strings/files
- Substitute parameters ({{param_name}}) with actual values
- Validate strategy structure using Pydantic models
"""

import json
import re
from pathlib import Path
from typing import Any, Dict, Union

import yaml
from loguru import logger
from pydantic import ValidationError

from app.strategy.schema import StrategyDefinition, StrategyWrapper


class StrategyParseError(Exception):
    """Raised when strategy parsing fails"""

    pass


class StrategyParser:
    """Parser for strategy definitions"""

    @staticmethod
    def from_yaml(yaml_str: str) -> StrategyDefinition:
        """
        Parse strategy from YAML string

        Args:
            yaml_str: YAML string containing strategy definition

        Returns:
            Validated StrategyDefinition object

        Raises:
            StrategyParseError: If parsing or validation fails
        """
        try:
            data = yaml.safe_load(yaml_str)
            return StrategyParser._validate_data(data)
        except yaml.YAMLError as e:
            raise StrategyParseError(f"Invalid YAML: {e}")
        except ValidationError as e:
            raise StrategyParseError(f"Validation error: {e}")

    @staticmethod
    def from_json(json_str: str) -> StrategyDefinition:
        """
        Parse strategy from JSON string

        Args:
            json_str: JSON string containing strategy definition

        Returns:
            Validated StrategyDefinition object

        Raises:
            StrategyParseError: If parsing or validation fails
        """
        try:
            data = json.loads(json_str)
            return StrategyParser._validate_data(data)
        except json.JSONDecodeError as e:
            raise StrategyParseError(f"Invalid JSON: {e}")
        except ValidationError as e:
            raise StrategyParseError(f"Validation error: {e}")

    @staticmethod
    def from_file(file_path: Union[str, Path]) -> StrategyDefinition:
        """
        Parse strategy from file

        Args:
            file_path: Path to YAML or JSON file

        Returns:
            Validated StrategyDefinition object

        Raises:
            StrategyParseError: If file not found or parsing fails
        """
        file_path = Path(file_path)

        if not file_path.exists():
            raise StrategyParseError(f"File not found: {file_path}")

        with open(file_path, "r") as f:
            content = f.read()

        # Detect format from extension
        if file_path.suffix.lower() in [".yaml", ".yml"]:
            return StrategyParser.from_yaml(content)
        elif file_path.suffix.lower() == ".json":
            return StrategyParser.from_json(content)
        else:
            # Try YAML first, fallback to JSON
            try:
                return StrategyParser.from_yaml(content)
            except StrategyParseError:
                return StrategyParser.from_json(content)

    @staticmethod
    def _validate_data(data: Dict[str, Any]) -> StrategyDefinition:
        """
        Validate data against schema

        Args:
            data: Parsed dictionary data

        Returns:
            Validated StrategyDefinition

        Raises:
            ValidationError: If validation fails
        """
        # Check if wrapped in "strategy" key
        strategy_data = data.get("strategy", data)

        # Perform parameter substitution BEFORE validation
        if "parameters" in strategy_data:
            parameters = strategy_data["parameters"]
            strategy_data = StrategyParser._recursive_substitute(strategy_data, parameters)

        # Now validate
        return StrategyDefinition(**strategy_data)

    @staticmethod
    def substitute_parameters(
        strategy: StrategyDefinition,
    ) -> StrategyDefinition:
        """
        Substitute parameter placeholders ({{param_name}}) with actual values

        Args:
            strategy: Strategy definition with parameters

        Returns:
            Strategy with substituted values
        """
        # Convert to dict
        strategy_dict = strategy.model_dump()

        # Perform substitution
        substituted_dict = StrategyParser._recursive_substitute(
            strategy_dict, strategy.parameters
        )

        # Re-validate
        return StrategyDefinition(**substituted_dict)

    @staticmethod
    def _recursive_substitute(
        obj: Any, parameters: Dict[str, Union[float, int, str]]
    ) -> Any:
        """
        Recursively substitute parameters in nested structures

        Args:
            obj: Object to substitute (can be dict, list, str, etc.)
            parameters: Parameter substitution map

        Returns:
            Object with substituted values
        """
        if isinstance(obj, dict):
            return {k: StrategyParser._recursive_substitute(v, parameters) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [StrategyParser._recursive_substitute(item, parameters) for item in obj]
        elif isinstance(obj, str):
            # Replace {{param_name}} with parameter value
            pattern = r"\{\{(\w+)\}\}"

            def replace_match(match):
                param_name = match.group(1)
                if param_name not in parameters:
                    raise StrategyParseError(
                        f"Parameter '{param_name}' referenced but not defined"
                    )
                return str(parameters[param_name])

            return re.sub(pattern, replace_match, obj)
        else:
            return obj

    @staticmethod
    def to_dict(strategy: StrategyDefinition) -> Dict[str, Any]:
        """
        Convert strategy definition to dictionary

        Args:
            strategy: Strategy definition

        Returns:
            Dictionary representation
        """
        return strategy.model_dump()

    @staticmethod
    def to_yaml(strategy: StrategyDefinition) -> str:
        """
        Convert strategy definition to YAML string

        Args:
            strategy: Strategy definition

        Returns:
            YAML string
        """
        data = {"strategy": strategy.model_dump()}
        return yaml.dump(data, default_flow_style=False, sort_keys=False)

    @staticmethod
    def to_json(strategy: StrategyDefinition, indent: int = 2) -> str:
        """
        Convert strategy definition to JSON string

        Args:
            strategy: Strategy definition
            indent: JSON indentation level

        Returns:
            JSON string
        """
        data = {"strategy": strategy.model_dump()}
        return json.dumps(data, indent=indent)


def parse_strategy(source: Union[str, Path, Dict]) -> StrategyDefinition:
    """
    Convenience function to parse strategy from various sources

    Args:
        source: Can be:
            - File path (str or Path)
            - YAML/JSON string
            - Dictionary

    Returns:
        Validated and substituted StrategyDefinition

    Examples:
        >>> # From file
        >>> strategy = parse_strategy("strategies/my_strategy.yaml")
        >>>
        >>> # From YAML string
        >>> yaml_str = '''
        ... strategy:
        ...   name: Test Strategy
        ...   version: 1.0.0
        ...   ...
        ... '''
        >>> strategy = parse_strategy(yaml_str)
        >>>
        >>> # From dict
        >>> strategy_dict = {...}
        >>> strategy = parse_strategy(strategy_dict)
    """
    try:
        # If it's a Path object or a string that looks like a file path
        if isinstance(source, Path) or (isinstance(source, str) and ("/" in source or "\\" in source or source.endswith((".yaml", ".yml", ".json")))):
            strategy = StrategyParser.from_file(source)
        # If it's a dictionary
        elif isinstance(source, dict):
            strategy = StrategyParser._validate_data(source)
        # If it's a string
        elif isinstance(source, str):
            # Try YAML first
            try:
                strategy = StrategyParser.from_yaml(source)
            except StrategyParseError:
                # Try JSON
                strategy = StrategyParser.from_json(source)
        else:
            raise StrategyParseError(f"Unsupported source type: {type(source)}")

        # Parameter substitution is already done in _validate_data
        logger.debug(f"Successfully parsed strategy: {strategy.name} v{strategy.version}")
        return strategy

    except Exception as e:
        logger.error(f"Failed to parse strategy: {e}")
        raise StrategyParseError(f"Failed to parse strategy: {e}")
