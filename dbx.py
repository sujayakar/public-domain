from queue import Queue
import collections
import dropbox
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

    def get(self, component):
        return self.children.get(component.lower())

    def get_or_insert_dir(self, component):
        cur = self.get(component)
        if cur is not None:
            return cur
        self.children[component.lower()] = cur = Directory()
        self.orig_names[component.lower()] = component
        return cur

    def insert(self, filename, entry):
        assert filename.lower() not in self.children
        assert filename.lower() not in self.orig_names
        self.children[filename.lower()] = entry
        self.orig_names[filename.lower()] = filename

    def drop(self, filename):
        self.children.pop(filename.lower(), None)
        self.orig_names.pop(filename.lower(), None)

class MetadataCache(object):
    def __init__(self, dbx, root):
        self._dbx = dbx
        self._root = root if root != '/' else ''
        self._lock = threading.Lock()
        self._tree = Directory()
        self._dirty_queue = Queue()

        self._thread = threading.Thread(target=self._list_thread)
        self._thread.daemon = True
        self._thread.start()

        self._cursor = None
        self._cursor_changed = threading.Condition(self._lock)

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
                    self._dirty_queue.put(path)
                    # Root we're currently listing
                    if not path:
                        assert isinstance(entry, dropbox.files.FolderMetadata)
                        continue
                    parent_node, filename = self._merge_parent(path)
                    if isinstance(entry, dropbox.files.FileMetadata):
                        if parent_node.get(filename):
                            parent_node.drop(filename)
                        parent_node.insert(filename, entry)
                    elif isinstance(entry, dropbox.files.FolderMetadata):
                        cur_entry = parent_node.get(filename)
                        if cur_entry is not None:
                            if isinstance(cur_entry, dropbox.files.FileMetadata):
                                parent_node.drop(filename)
                        else:
                            parent_node.insert(filename, Directory())
                    else:
                        assert isinstance(entry, dropbox.files.DeletedMetadata)
                        parent_node.drop(filename)

                # Publish the new cursor to the rest of the system
                cursor = self._cursor = resp.cursor
                self._cursor_changed.notifyAll()

                if not resp.has_more:
                    return cursor

    def _list_thread(self, cursor=None):
        while True:
            if cursor is not None:
                resp = self._dbx.files_list_folder_longpoll(cursor)
                print("Woke up from subscribe, listing...")
                if resp.backoff:
                    time.sleep(resp.backoff)
            if cursor is None or resp.changes:
                cursor = self._list(cursor)

    def _merge_parent(self, path):
        components = path.strip('/').split('/')
        assert components
        node = self._tree
        for component in components[:-1]:
            node = node.get_or_insert_dir(component)
            if not isinstance(node, Directory):
                raise Exception("Path %s underneath file" % path)
        return node, components[-1]

    def _find(self, path):
        path = path.strip('/')
        node = self._tree
        if path:
            for component in path.split('/'):
                if node is None:
                    return None
                node = node.get(component)
        return node

    def stat(self, path):
        with self._lock:
            node = self._find(path)
        if not isinstance(node, dropbox.files.FileMetadata):
            raise IsDirError("Can only stat files")
        return node

    def listdir(self, path):
        with self._lock:
            node = self._find(path)
            if not isinstance(node, Directory):
                raise IsFileError("Can only list directories: '%s', %s" % (path, node))
            children = [
                (node.orig_names[k], v)
                for k, v in sorted(node.children.items())
            ]
            return children, self._cursor

    def _from_rr(self, path):
        # AMERICUH
        assert path.lower().startswith(self._root.lower())
        return path[len(self._root):]

class DBXFolder(object):
    def __init__(self, config):
        self._root = config['root']
        if self._root != '/':
            assert self._root.startswith('/') and not self._root.endswith('/')
        self._dbx = dropbox.Dropbox(config['access_token'])
        self.cache = MetadataCache(self._dbx, self._root)

    def download(self, st):
        if st is None:
            return None
        _, resp = self._dbx.files_download(st.path_display)
        assert resp.ok
        return resp

    def listdir(self, path):
        return self.cache.listdir(path)

    def subscribe(self, cursor):
        with self.cache._lock:
            if cursor != self.cache._cursor:
                return True

            self.cache._cursor_changed.wait(15.)
            return cursor != self.cache._cursor
