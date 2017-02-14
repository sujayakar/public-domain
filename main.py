from dbx import PublicFolder
from flask import Flask, Response, abort
import dropbox
import logging

app = Flask(__name__)
pf = PublicFolder('/home/sujayakar/secret.json')
CHUNK_SIZE = 1 << 22

@app.route("/Public/", methods=['GET'])
@app.route("/Public/<path:dbx_path>", methods=['GET'])
def public_folder(dbx_path=''):
    st = pf.stat(dbx_path)
    if isinstance(st, dropbox.files.FolderMetadata):
        return '<br/>'.join(
            '%s (%s)' % (p, st.__class__.__name__)
            for p, st in sorted(pf.listdir(dbx_path).items())
        )
    else:
        resp = pf.download(dbx_path)
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
        headers = {
            'Content-Length': resp.headers['Content-Length'],
            'Content-Type': resp.headers['Content-Type'],
        }
        return Response(generate(resp), headers=headers)
