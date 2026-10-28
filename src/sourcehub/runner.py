class IngestionFailed(Exception):
    def __init__(self, failures):
        self.failures = failures
        super().__init__(f"{len(failures)} source(s) failed: {', '.join(sorted(failures))}")


def run_sources(sources):
    # One failing account must not stop the rest, but it must never be silent either.
    results, failures = {}, {}
    for name, fetch in sources.items():
        try:
            results[name] = fetch()
        except Exception as exc:
            failures[name] = repr(exc)
    return results, failures


def raise_if_failed(failures):
    if failures:
        raise IngestionFailed(failures)
