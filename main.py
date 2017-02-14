from dbx import PublicFolder, IsFileError
from flask import Flask, Response, abort, render_template
import dropbox
import logging
import mimetypes
import os.path

app = Flask(__name__)
pf = PublicFolder('/home/sujayakar/secret.json')
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
        return render_template("folder.html",
                               title=title,
                               entries=entries)
    except IsFileError:
        st, resp = pf.download(dbx_path)
        if resp is None:
            abort(404)
        def generate(resp):
            try:
                for chunk in resp.iter_content(chunk_size=CHUNK_SIZE):
                    print("Received chunk of size %d" % len(chunk))
                    yield chunk
            finally:
                resp.close()
        # TODO: support range requests
        # TODO: support etag cache
        # TODO: pipeline downloads
        headers = {
            'Content-Length': resp.headers['Content-Length'],
        }
        filename = os.path.basename(st.path_display)
        mime_type, _ = mimetypes.guess_type(filename)
        if mime_type is not None:
            headers['Content-Type'] = mime_type
            headers['Content-Disposition'] = 'inline'
        else:
            headers['Content-Type'] = resp.headers['Content-Type']
            headers['Content-Disposition'] = 'attachment'
        return Response(generate(resp), headers=headers)
