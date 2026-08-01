"""Release Safety team's canary analysis platform.

A separate system from Waypoint's release pipeline: its own Temporal namespace,
its own task queue, its own worker process, its own deploy cadence. It talks to
Agent Gateway over the wire and shares no internal Python code with it.

Nothing in this package may import from gateway/, workflows/, activities/, or
common/. If you find yourself wanting to, the thing you want is a wire type, and
it belongs in release_safety/models.py as this team's own copy of the contract.
"""
