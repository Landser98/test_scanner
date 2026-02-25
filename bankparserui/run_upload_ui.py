#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Run the new upload UI for bank statements.
"""

import subprocess
import sys
from pathlib import Path

if __name__ == "__main__":
    # Run streamlit with the upload app
    upload_app_path = Path(__file__).parent / "src" / "ui" / "upload_app.py"
    
    cmd = [
        sys.executable,
        "-m",
        "streamlit",
        "run",
        str(upload_app_path),
        "--server.port=8502",
        "--server.address=0.0.0.0",
        "--server.headless=true"
    ]
    
    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as e:
        print(f"❌ Ошибка при запуске Streamlit: {e}", file=sys.stderr)
        sys.exit(1)
    except KeyboardInterrupt:
        print("\n🛑 Остановка Streamlit...")
        sys.exit(0)
