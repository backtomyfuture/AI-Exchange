import inspect


def test_get_retriever_returns_singleton():
    """get_retriever() should return the same instance every call."""
    from src.utils.retriever import get_retriever

    r1 = get_retriever()
    r2 = get_retriever()
    assert r1 is r2, "get_retriever() should return singleton instance"


def test_retriever_node_uses_singleton():
    """retriever_node should not instantiate EmailRetriever directly."""
    import src.nodes.retriever_node as mod

    source = inspect.getsource(mod.retrieve_context)
    assert "EmailRetriever()" not in source, (
        "retriever_node should use get_retriever() singleton, not EmailRetriever()"
    )


def test_retriever_node_uses_routing_engine_singleton():
    """retriever_node should use get_routing_engine(), not RoutingEngine()."""
    import src.nodes.retriever_node as mod

    source = inspect.getsource(mod.retrieve_context)
    assert "RoutingEngine()" not in source, (
        "retriever_node should use get_routing_engine() singleton, not RoutingEngine()"
    )
