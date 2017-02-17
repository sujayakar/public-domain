from dbx import DBXFolder, IsDirError, IsFileError
from flask import Flask, Response, abort, render_template, redirect, request
import dropbox
import json
import logging
import mimetypes
import os
import requests
import web

app = Flask(__name__)
config = web.Config('config.json')
dbx_folder = DBXFolder(config)
etags = web.ETagCache(dbx_folder)
templinks = web.TempLinkCache(dbx_folder)
blockcache = web.BlockCache(dbx_folder, config)
CHUNK_SIZE = 1 << 22

@app.route("/Public/", methods=['GET'])
@app.route("/Public/<path:dbx_path>", methods=['GET'])
def list_folder(dbx_path=''):
    try:
        title = os.path.join(config['root'], dbx_path)
        children, cursor = dbx_folder.listdir(dbx_path)
        entries = [
            (fname, ent, '/' + os.path.join(dbx_path, fname))
            for fname, ent in children
        ]
        return render_template("folder.html", title=title,
                               entries=entries, cursor=str(cursor))
    except IsFileError:
        pass

    try:
        etag = request.headers.get('If-None-Match')
        if etag is not None and etags.is_current(dbx_path, etag):
            print('Etag cache hit on %s!' % (dbx_path,))
            return Response(status=304)

        if request.headers.get('Range'):
            print("Serving range response for %s" % (dbx_path,))
            return range_download(dbx_path)

        return simple_download(dbx_path)
    except IsDirError:
        return Response(status=404)

@app.route("/subscribe/<cursor>", methods=['GET'])
def subscribe(cursor):
    resp = {'result': 'refresh' if dbx_folder.subscribe(cursor) else 'ok'}
    return json.dumps(resp)

def simple_download(dbx_path):
    res = blockcache.get(dbx_path)
    if res is None:
        return Response(status=404)
    st, headers, stream = res
    etags.register(dbx_path, st, headers['ETag'])

    filename = os.path.basename(st.path_display)
    mime_type, _ = mimetypes.guess_type(filename)
    if mime_type is not None:
        headers['Content-Type'] = mime_type
        headers['Content-Disposition'] = 'inline'
    else:
        headers['Content-Disposition'] = 'attachment'
    return Response(stream, headers=headers)

def range_download(dbx_path):
    url = templinks.get(dbx_path)
    if url is None:
        return Response(status=404)
    # Why bother wasting our bandwidth? Just let the client figure it out
    # since latency doesn't matter.
    return redirect(url, code=302)
