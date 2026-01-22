# api/index.py
# This file is the Vercel entrypoint. It imports the Flask app defined in app.py.
# Make sure app.py defines: app = Flask(__name__)

from app import app  # Import the Flask instance from your existing app.py

# Exported variable must be named `app`. Nothing else needed.
