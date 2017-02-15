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
pf = DBXFolder(root, '/home/sujayakar/secret.json')
etags = web.ETagCache(pf)
templinks = web.TempLinkCache(pf)
CHUNK_SIZE = 1 << 22

@app.route("/Public/", methods=['GET'])
@app.route("/Public/<path:dbx_path>", methods=['GET'])
def public_folder(dbx_path=''):
    try:
        title = os.path.join('Public/', dbx_path)
        entries = [
            (fname, ent, os.path.join('/Public', dbx_path, fname))
            for fname, ent in pf.listdir(dbx_path)
        ]
        return render_template("folder.html", title=title, entries=entries)
    except IsFileError:
        print('Download headers: %s' % dict(request.headers))
        etag = request.headers.get('If-None-Match')
        if etag is not None and etags.is_current(dbx_path, etag):
            print('Cache hit on %s!' % (dbx_path,))
            return Response(status=304)

        if request.headers.get('Range'):
            return range_download(dbx_path, req_range)

        return simple_download(dbx_path)

def simple_download(dbx_path):
    st, resp = pf.download(dbx_path)
    if resp is None:
        return Response(status=404)

    # TODO: support range requests
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

def range_download(dbx_path, range_hdr):
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
