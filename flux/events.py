import inspect
import traceback


def arity(fn):
    """How many positional args a handler accepts, so handlers can take fewer args."""
    try:
        params = inspect.signature(fn).parameters.values()
    except (TypeError, ValueError):
        return 99
    n = 0
    for p in params:
        if p.kind == p.VAR_POSITIONAL:
            return 99
        if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD):
            n += 1
    return n


class Events:
    def __init__(self):
        self._handlers = {}

    def on(self, event, fn=None):
        """Use as @obj.on("event") or obj.on("event", fn)."""
        def register(f):
            self._handlers.setdefault(event, []).append((f, arity(f)))
            return f
        return register(fn) if fn else register

    def _fire(self, event, *args):
        for fn, n in list(self._handlers.get(event, ())):
            try:
                fn(*args[:n])
            except Exception:
                traceback.print_exc()
