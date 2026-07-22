"""Durable ingestion package.

Callers import the owning module directly so importing a model does not load
cold-start, synchronization, command-receipt, and repository implementations.
"""
