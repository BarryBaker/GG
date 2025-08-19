#!/usr/bin/env python3
"""Gunicorn launcher that reads PORT from env without shell expansion."""

import os
import sys

def main():
    from gunicorn.app.wsgiapp import run

    port = os.getenv("PORT", "8000")
    # Build argv for gunicorn: gunicorn -b 0.0.0.0:<port> api:app
    sys.argv = [
        "gunicorn",
        "-b",
        f"0.0.0.0:{port}",
        "api:app",
    ]
    run()


if __name__ == "__main__":
    main()


