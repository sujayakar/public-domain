import threading

class ETagCache(object):
    def __init__(self, pf):
        self._lock = threading.Lock()
        self._pf = pf
        self._cache = {}

    def register(self, path, st, tag):
        with self._lock:
            self._cache[path] = (st.rev, tag)

    def is_current(self, path, tag):
        with self._lock:
            if path not in self._cache:
                return False

            cur_rev, cur_tag = self._cache[path]

            # First, check that the tag is current
            if cur_tag != tag:
                return False

            # Next, check that the cur_tag is itself current
            st = self._pf.cache.stat(path)
            if st is None or st.rev != cur_rev:
                del self._cache[path]
                return False

            return True
