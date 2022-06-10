from flask import request, Blueprint

from controllers.file_processor import FileProcessor, ProcessingError

load = Blueprint("load", __name__)


@load.route("/load")
def load_file():
    endpoint = request.args.get("path")
    destination = request.args.get("destination")
    if not endpoint:
        return "No path provided", 400

    file_processor = FileProcessor(destination)
    try:
        result = file_processor.process(endpoint)
    except ProcessingError as e:
        return e.msg, 500

    return result
