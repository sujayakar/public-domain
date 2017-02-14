import collections
import dropbox
import json
import logging
import os

class PublicFolder(object):
    def __init__(self, secret_file):
        secret = json.load(open(secret_file))
        self._dbx = dropbox.Dropbox(secret['access_token'])

        self._root = u'/Public'
        self._metadata_cache = {}
        self._children_cache = {}
        self._cursor = None

    def download(self, path):
        st = self.stat(path)
        if st is None:
            return None
        print("Downloading %s..." % path)
        _, resp = self._dbx.files_download(st.path_display)
        assert resp.ok
        return resp

    def stat(self, path):
        return self._get_metadata(path)

    def listdir(self, path):
        st = self.stat(path)
        if not isinstance(st, dropbox.files.FolderMetadata):
            raise Exception("Not a directory")

        return {
            p: self.stat(p)
            for p in self._children_cache[path.lower()]
        }

    def _get_metadata(self, path):
        try:
            return self._metadata_cache[path.lower()]
        except KeyError:
            pass

        try:
            print('Loading metadata for "{}"'.format(path, self._to_rr(path)))
            md = self._dbx.files_get_metadata(self._to_rr(path))
        except dropbox.exceptions.ApiError as api_e:
            p_error = api_e.error.get_path()
            if isinstance(p_error, dropbox.files.LookupError) and p_error.is_not_found():
                return None
            raise

        if isinstance(md, dropbox.files.FolderMetadata):
            children = []
            for entry in self._list_folder(path):
                p = self._from_rr(entry.path_display)
                children.append(p)
                self._metadata_cache[p.lower()] = entry

            self._children_cache[path.lower()] = children

        self._metadata_cache[path.lower()] = md
        return md

    def _list_folder(self, path):
        result = self._dbx.files_list_folder(self._to_rr(path))
        for entry in result.entries:
            yield entry
        cursor = result.cursor if result.has_more else None
        while cursor is not None:
            result = self._dbx.files_list_folder_continue(cursor)
            for entry in result.entries:
                yield entry
            cursor = result.cursor if result.has_more else None

    def _to_rr(self, path):
        assert '..' not in path
        return os.path.join(self._root, path).rstrip('/')

    def _from_rr(self, path):
        assert path.startswith(self._root)
        return os.path.relpath(path, self._root)
