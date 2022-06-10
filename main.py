from flask import Flask
from paths.load_file import load

app = Flask(__name__)
app.register_blueprint(load)

def main():
    app.run(host="0.0.0.0", port="5000")


if __name__ == "__main__":
    main()
