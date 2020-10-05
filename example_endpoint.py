import time

import flask

app = flask.Flask(__name__)


@app.route("/<path>", methods=['POST'])
def handle_job(path):
    print("job received at path:", path, "body:", flask.request.data.decode())
    time.sleep(5)
    return ""


app.run(use_reloader=True)
