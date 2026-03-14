"""
Tests for jerry_trader.config.load_prompt

Covers:
    - Loading existing prompt files from skills/
    - FileNotFoundError on missing files
    - Return type and encoding
"""

import pytest

from jerry_trader.platform.config.config import load_prompt


class TestLoadPrompt:
    """Tests for load_prompt function."""

    def test_load_existing_prompt(self):
        """Should load the catalyst_news_judge.md successfully."""
        text = load_prompt("catalyst_news_judge.md")
        assert isinstance(text, str)
        assert len(text) > 0

    def test_load_system_prompt(self):
        """Should load the news_processor_system_prompt.md successfully."""
        text = load_prompt("news_processor_system_prompt.md")
        assert isinstance(text, str)
        assert len(text) > 0

    def test_missing_file_raises(self):
        """Non-existent prompt file → FileNotFoundError."""
        with pytest.raises(FileNotFoundError, match="Prompt file not found"):
            load_prompt("this_does_not_exist.md")

    def test_missing_file_lists_available(self):
        """Error message should list available prompt files."""
        with pytest.raises(FileNotFoundError, match="Available prompts"):
            load_prompt("nonexistent.md")

    def test_returns_string_utf8(self):
        """Content should be valid UTF-8 string."""
        text = load_prompt("catalyst_news_judge.md")
        # Should not raise — if it loaded, it's valid UTF-8
        text.encode("utf-8")
