import collections
import dropbox
import json
import logging
import os
import threading
import time

class IsDirError(Exception):
    pass

class IsFileError(Exception):
    pass

class Directory(object):
    def __init__(self):
        self.children = {}
        # lowered -> original
        self.orig_names = {}

class MetadataCache(object):
    def __init__(self, dbx, root):
        self._dbx = dbx
        self._root = root
        self._lock = threading.Lock()
        self._tree = {}

        cursor = self._list()
        self._thread = threading.Thread(target=self._list_thread, args=(cursor,))
        self._thread.daemon = True
        self._thread.start()

    def _list(self, cursor=None):
        while True:
            if cursor is None:
                resp = self._dbx.files_list_folder(
                    self._root,
                    recursive=True,
                    include_deleted=True,
                )
            else:
                resp = self._dbx.files_list_folder_continue(cursor)
            print("Listed %d entries..." % len(resp.entries))
            with self._lock:
                for entry in resp.entries:
                    path = self._from_rr(entry.path_display)
                    # Root we're currently listing
                    if not path:
                        assert isinstance(entry, dropbox.files.FolderMetadata)
                        continue
                    parent_node, filename_l = self._merge_parent(path)
                    if isinstance(entry, dropbox.files.FileMetadata):
                        if filename_l in parent_node:
                            del parent_node[filename_l]
                        parent_node[filename_l] = entry
                    elif isinstance(entry, dropbox.files.FolderMetadata):
                        if filename_l in parent_node:
                            cur_entry = parent_node[filename_l]
                            if isinstance(cur_entry, dropbox.files.FileMetadata):
                                del parent_node[filename_l]
                        else:
                            parent_node[filename_l] = {}
                    else:
                        assert isinstance(entry, dropbox.files.DeletedMetadata)
                        parent_node.pop(filename_l, None)

            cursor = resp.cursor
            if not resp.has_more:
                return cursor

    def _list_thread(self, cursor):
        while True:
            resp = self._dbx.files_list_folder_longpoll(cursor)
            print("Woke up from subscribe, listing...")
            if resp.backoff:
                time.sleep(resp.backoff)
            if resp.changes:
                cursor = self._list(cursor)

    def _merge_parent(self, path):
        path = path.strip('/')
        assert path
        node = self._tree
        components = path.lower().split('/')
        for component in components[:-1]:
            if component not in node:
                node[component] = {}
            node = node[component]
        return node, components[-1]

    def stat(self, path):
        path = path.strip('/')
        if not path:
            raise Exception("Can't stat root directory")
        assert path
        with self._lock:
            node = self._tree
            for component in path.lower().split('/'):
                if component not in node:
                    return None
                node = node[component]

        if not isinstance(node, dropbox.files.FileMetadata):
            raise IsDirError("Can only stat files")

        return node

    def listdir(self, path):
        path = path.strip('/')
        with self._lock:
            node = self._tree
            if path:
                for component in path.lower().split('/'):
                    if component not in node:
                        return None
                    node = node[component]
        if not isinstance(node, dict):
            raise IsFileError("Can only list directories")

        result = []
        for key in sorted(node.keys()):
            entry = node[key]
            if isinstance(entry, dict):
                # XXX
                fn, entry = key, Directory
            else:
                fn, entry = entry.path_display.split('/')[-1], entry
            result.append((fn, entry))
        return result

    def _to_rr(self, path):
        assert '..' not in path
        return os.path.join(self._root, path).strip('/')

    def _from_rr(self, path):
        # AMERICUH
        assert path.lower().startswith(self._root.lower())
        return path[len(self._root):]

class PublicFolder(object):
    def __init__(self, secret_file):
        secret = json.load(open(secret_file))
        self._dbx = dropbox.Dropbox(secret['access_token'])
        self._root = '/Public'
        self.cache = MetadataCache(self._dbx, self._root)

    def download(self, path):
        st = self.cache.stat(path)
        if st is None:
            return None
        print("Downloading %s..." % path)
        _, resp = self._dbx.files_download(st.path_display)
        assert resp.ok
        return resp

    def listdir(self, path):
        return self.cache.listdir(path)
