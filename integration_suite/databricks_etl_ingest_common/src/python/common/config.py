import configparser
import re
import os
from typing import Dict, Optional
from databricks.sdk.runtime import dbutils


class DatabricksConfig:
    """
    Common class to read and manage Databricks configuration with secret resolution.

    Supports:
    - Reading from config files (INI format)
    - Reading from config strings
    - Resolving Databricks secrets in format {{secrets/scope/key}}
    - Environment variable fallback
    """

    def __init__(self, config_path: Optional[str] = None, config_string: Optional[str] = None):
        """
        Initialize the configuration reader.

        Args:
            config_path: Path to configuration file
            config_string: Configuration content as string
        """
        self.config = configparser.ConfigParser(allow_no_value=True)

        if config_path:
            if not os.path.exists(config_path):
                raise FileNotFoundError(f"Configuration file not found: {config_path}")
            try:
                self.config.read(config_path)
            except configparser.DuplicateOptionError as e:
                raise ValueError(f"Duplicate option in config file: {e}")
        elif config_string:
            try:
                self.config.read_string(config_string)
            except configparser.DuplicateOptionError as e:
                raise ValueError(f"Duplicate option in config string: {e}")
        else:
            raise ValueError("Either config_path or config_string must be provided")

    def _resolve_secret(self, value: str) -> str:
        """
        Resolve Databricks secret references in format {{secrets/scope/key}}

        Args:
            value: Configuration value that may contain secret reference

        Returns:
            Resolved value with secrets substituted
        """
        if not isinstance(value, str):
            return value

        # Remove quotes if present
        value = value.strip("\"'")

        # Pattern to match secret references
        pattern = r"\{\{secrets/([^/]+)/([^}]+)\}\}"
        match = re.match(pattern, value)

        if match:
            scope, key = match.groups()
            return dbutils.secrets.get(scope=scope, key=key)

        return value

    def get(self, section: str, key: str, fallback: Optional[str] = None) -> str:
        """
        Get configuration value with secret resolution.

        Args:
            section: Configuration section name
            key: Configuration key name
            fallback: Default value if key not found

        Returns:
            Resolved configuration value
        """
        try:
            raw_value = self.config[section][key]
            return self._resolve_secret(raw_value)
        except KeyError:
            if fallback is not None:
                return fallback
            raise KeyError(f"Key '{key}' not found in section '{section}'")

    def get_section(self, section: str) -> Dict[str, str]:
        """
        Get entire section with resolved secrets.

        Args:
            section: Configuration section name

        Returns:
            Dictionary of resolved configuration values
        """
        if section not in self.config:
            raise KeyError(f"Section '{section}' not found")

        section_dict = {}
        for key in self.config[section]:
            section_dict[key] = self.get(section, key)
        return section_dict

    def list_sections(self) -> list:
        """
        List all available configuration sections.

        Returns:
            List of section names
        """
        return list(self.config.sections())

    def has_section(self, section: str) -> bool:
        """
        Check if section exists in configuration.

        Args:
            section: Section name to check

        Returns:
            True if section exists, False otherwise
        """
        return section in self.config

    def has_key(self, section: str, key: str) -> bool:
        """
        Check if key exists in section.

        Args:
            section: Section name
            key: Key name to check

        Returns:
            True if key exists, False otherwise
        """
        return section in self.config and key in self.config[section]
