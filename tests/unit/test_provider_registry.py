"""Provider registry tests — OAuth extension."""

from src.providers.registry import ProviderSpec, match_provider, PROVIDERS


class TestProviderSpecOAuthFields:
    """Test that ProviderSpec has is_oauth and is_direct fields."""

    def test_default_is_oauth_false(self):
        spec = ProviderSpec(name="test")
        assert spec.is_oauth is False

    def test_default_is_direct_false(self):
        spec = ProviderSpec(name="test")
        assert spec.is_direct is False

    def test_oauth_provider_spec(self):
        spec = ProviderSpec(name="test_oauth", is_oauth=True, is_direct=True)
        assert spec.is_oauth is True
        assert spec.is_direct is True


class TestOAuthProviderRegistration:
    """Test that OAuth providers are registered and matchable."""

    def test_codex_in_registry(self):
        names = [p.name for p in PROVIDERS]
        assert "openai_codex" in names

    def test_gemini_cli_in_registry(self):
        names = [p.name for p in PROVIDERS]
        assert "gemini_cli" in names

    def test_codex_match_by_keyword(self):
        spec = match_provider("openai-codex/gpt-5.1-codex")
        assert spec is not None
        assert spec.name == "openai_codex"
        assert spec.is_oauth is True

    def test_gemini_cli_match_by_keyword(self):
        spec = match_provider("gemini-cli/gemini-2.5-flash")
        assert spec is not None
        assert spec.name == "gemini_cli"
        assert spec.is_oauth is True

    def test_existing_providers_unchanged(self):
        spec = match_provider("gpt-4o")
        assert spec is not None
        assert spec.name == "openai"
        assert spec.is_oauth is False
