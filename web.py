import threading
import time

class ETagCache(object):
    def __init__(self, dbx_folder):
        self._lock = threading.Lock()
        self._dbx_folder = dbx_folder
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
            st = self._dbx_folder.cache.stat(path)
            if st is None or st.rev != cur_rev:
                del self._cache[path]
                return False

            return True

class TempLinkCache(object):
    EXPIRATION = 60 * 60 * 3

    def __init__(self, dbx_folder):
        self._lock = threading.Lock()
        self._dbx_folder = dbx_folder
        self._cache = {}

    def get(self, path):
        st = self._dbx_folder.cache.stat(path)
        if st is None:
            return None

        with self._lock:
            try:
                cur_rev, expires, url = self._cache[path]
                if st.rev == cur_rev and time.time() < expires:
                    return url
                else:
                    del self._cache[path]
            except KeyError:
                pass

        print("Fetching templink for %s..." % path)
        url = self._dbx_folder._dbx.files_get_temporary_link(st.path_display).link
        with self._lock:
            expiration = time.time() + self.EXPIRATION
            self._cache[path] = (st.rev, expiration, url)

        return url

def parse_range(range_hdr):
    assert range_hdr.startswith('bytes=')
    payload = range_hdr.partition('bytes=')[2].strip()
    assert payload.count('-') == 1
    lower, _, upper = payload.partition('-')
    if not lower:
        lower = None
    else:
        lower = int(lower)
    if not upper:
        upper = None
    else:
        upper = int(upper)
    return (lower, upper)
