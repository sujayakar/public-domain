from dbx import DBXFolder, IsFileError
from flask import Flask, Response, abort, render_template, redirect, request
import dropbox
import logging
import mimetypes
import os.path
import requests
import web

app = Flask(__name__)
root = '/Public'
dbx_folder = DBXFolder(root, '/home/sujayakar/secret.json')
etags = web.ETagCache(dbx_folder)
templinks = web.TempLinkCache(dbx_folder)
CHUNK_SIZE = 1 << 22

assert root.startswith('/') and not root.endswith('/')

@app.route("%s/" % root, methods=['GET'])
@app.route("%s/<path:dbx_path>" % root, methods=['GET'])
def list_folder(dbx_path=''):
    try:
        title = os.path.join(root, dbx_path)
        entries = [
            (fname, ent, os.path.join(root, dbx_path, fname))
            for fname, ent in dbx_folder.listdir(dbx_path)
        ]
        return render_template("folder.html", title=title, entries=entries)
    except IsFileError:
        print('Download headers: %s' % dict(request.headers))
        etag = request.headers.get('If-None-Match')
        if etag is not None and etags.is_current(dbx_path, etag):
            print('Cache hit on %s!' % (dbx_path,))
            return Response(status=304)

        if request.headers.get('Range'):
            return range_download(dbx_path)

        return simple_download(dbx_path)

def simple_download(dbx_path):
    st, resp = dbx_folder.download(dbx_path)
    if resp is None:
        return Response(status=404)

    headers = {
        'Content-Length': resp.headers['Content-Length'],
        'ETag': resp.headers['ETag'],
    }
    etags.register(dbx_path, st, resp.headers['ETag'])

    filename = os.path.basename(st.path_display)
    mime_type, _ = mimetypes.guess_type(filename)
    if mime_type is not None:
        headers['Content-Type'] = mime_type
        headers['Content-Disposition'] = 'inline'
    else:
        headers['Content-Type'] = resp.headers['Content-Type']
        headers['Content-Disposition'] = 'attachment'
    return Response(generate(dbx_path, resp), headers=headers)

def range_download(dbx_path):
    url = templinks.get(dbx_path)
    if url is None:
        return Response(status=404)
    # Why bother wasting our bandwidth? Just let the client figure it out
    # since latency doesn't matter.
    return redirect(url, code=302)

def generate(dbx_path, resp):
    try:
        for chunk in resp.iter_content(chunk_size=CHUNK_SIZE):
            print("Received chunk of size %d for %s" % (len(chunk), dbx_path))
            yield chunk
    finally:
        resp.close()
