import hashlib
import json
import os
import threading
import time

from dbx import IsDirError

class Config(object):
    def __init__(self, config_file):
        self._raw = json.load(open(config_file))

    def __getitem__(self, key):
        return self._raw[key]

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

class BlockCache(object):
    def __init__(self, dbx, config):
        self._dbx = dbx
        self._cache_dir = config['blockcache']
        self._prefetch_size = config['prefetch'] * (1 << 10)
        self._cacheable_size = config['cacheable'] * (1 << 20)
        self._max_size = config['cache_size'] * (1 << 20)
        self._chunk_size = config['chunk_size'] * (1 << 20)

        for p in os.listdir(self._cache_dir):
            os.unlink(os.path.join(self._cache_dir, p))

        self._lock = threading.Lock()
        # path -> (rev, size, last access, resp_headers, diskpath)
        self._cache = {}
        self._size = 0

        self._dirty_queue = dbx.cache._dirty_queue
        self._threads = []
        for _ in range(config['prefetch_threads']):
            t = threading.Thread(target=self._prefetch_loop)
            self._threads.append(t)
            t.daemon = True
            t.start()

    def get(self, path):
        """
        Returns Option<(st, headers, content generator)>
        """
        st = self._dbx.cache.stat(path)
        if st is None:
            with self._lock:
                self._clear(path)
            return None

        with self._lock:
            cache_result = self._lookup(path, st)
        if cache_result is not None:
            print('Block cache hit on %s!' % path)
            headers, diskfd = cache_result
            return st, headers, self._stream_file(diskfd)

        # Too big to cache
        if st.size > self._cacheable_size:
            print('%s too big to cache' % path)
            resp_headers, stream = self._download(st)
            return st, resp_headers, stream

        # Okay, let's cache this.  We need to reserve our space, potentially
        # evicting other entries.  If we fail after allocating, we'll leak space
        # in the cache.
        with self._lock:
            # XXX: This isn't quite right (round up to 4kb)
            self._allocate(st.size)

        # Stream the file in and write it to disk
        resp_headers, stream = self._download(st)
        def write_cache(resp_headers, stream):
            p = os.path.join(self._cache_dir, self._cache_name(st))
            with open(p, 'wb') as f:
                for chunk in stream:
                    f.write(chunk)
                    yield chunk
            with self._lock:
                self._cache[path] = (st.rev, st.size, time.time(), resp_headers, p)

        return st, resp_headers, write_cache(resp_headers, stream)

    def prime(self, path):
        st = self._dbx.cache.stat(path)
        if st is None:
            return

        if st.size > self._prefetch_size:
            return

        with self._lock:
            if self._lookup(path, st) is not None:
                return

        print("Priming %s..." % path)
        res = self.get(path)
        if res is None:
            return
        _, _, stream = res
        # Drain the stream so it fills the cache
        for block in stream:
            pass

    def _prefetch_loop(self):
        while True:
            try:
                self.prime(self._dirty_queue.get())
            except IsDirError:
                pass

    def _download(self, st):
        resp = self._dbx.download(st)
        resp_headers = {
            'Content-Length': resp.headers['Content-Length'],
            'ETag': resp.headers['ETag'],
            'Accept-Ranges': 'bytes',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache',
        }
        def stream(resp):
            try:
                for chunk in resp.iter_content(chunk_size=self._chunk_size):
                    print("Received chunk of size %d for %s" % (
                        len(chunk), st.path_display))
                    yield chunk
            finally:
                resp.close()
        return resp_headers, stream(resp)

    def _clear(self, path):
        if path not in self._cache:
            return

        _, size, _, _, p = self._cache.pop(path)
        print("Cache evicted %s (%0.2f KiB)" % (path, size / float(1 << 10)))
        os.unlink(p)
        self._size -= size
        return size

    def _lookup(self, path, st):
        try:
            (rev, size, last_access, resp_headers, p) = self._cache[path]
        except KeyError:
            return None

        if st.rev != rev:
            self._clear(path)
            return None

        self._cache[path] = (rev, size, time.time(), resp_headers, p)
        return resp_headers, open(p, 'rb')

    def _stream_file(self, fd):
        try:
            while True:
                buf = fd.read(self._chunk_size)
                if not buf:
                    break
                yield buf
        finally:
            fd.close()

    def _allocate(self, size):
        target_size = self._size + size
        if target_size > self._max_size:
            to_free = target_size - self._max_size
            while to_free > 0:
                _, key = min(
                    (last_access, key)
                    for key, (_, _, last_access, _, _) in self._cache.iteritems()
                )
                to_free -= self._clear(key)
        self._size += size
        print("Cache allocated %0.2f KiB (total size %0.2fMiB)" % (
            size / float(1 << 10), self._size / float(1 << 20)))

    def _cache_name(self, st):
        h = hashlib.md5(st.path_display.encode('utf-8'))
        h.update(st.rev.encode('utf-8'))
        return h.hexdigest()
