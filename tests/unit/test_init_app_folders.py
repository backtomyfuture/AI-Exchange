import inspect


def test_setup_async_calls_get_all_folders():
    from src.init_app import AppContext

    source = inspect.getsource(AppContext.setup_async)
    assert "get_all_folders" in source, (
        "setup_async should call exchange_client.get_all_folders() during startup"
    )


def test_setup_async_calls_init_folder_policies():
    from src.init_app import AppContext

    source = inspect.getsource(AppContext.setup_async)
    assert "init_folder_policies" in source, (
        "setup_async should call exchange_client.init_folder_policies() during startup"
    )
