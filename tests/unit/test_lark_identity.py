from types import SimpleNamespace

from src.utils import lark_app


class _FakeChatApi:
    def get(self, _request):
        return SimpleNamespace(
            success=lambda: True,
            data=SimpleNamespace(owner_id="ou_owner"),
        )


class _FakeUserApi:
    def get(self, _request):
        return SimpleNamespace(
            success=lambda: True,
            data=SimpleNamespace(user=SimpleNamespace(email="q-fu@hnair.com")),
        )


def test_lark_identity_resolution_does_not_rewrite_exchange_account_email(monkeypatch):
    settings = SimpleNamespace(EXCHANGE_ACCOUNT_EMAIL="q-fu@tianjin-air.com")
    fake_client = SimpleNamespace(
        im=SimpleNamespace(v1=SimpleNamespace(chat=_FakeChatApi())),
        contact=SimpleNamespace(v3=SimpleNamespace(user=_FakeUserApi())),
    )

    monkeypatch.setattr(lark_app, "lark_api_client", fake_client)
    monkeypatch.setattr(lark_app, "get_settings", lambda: settings)

    lark_app._resolve_current_user_email("oc_chat")

    assert settings.EXCHANGE_ACCOUNT_EMAIL == "q-fu@tianjin-air.com"
