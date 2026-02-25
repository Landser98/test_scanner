#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Run the taxpayer search UI.
"""

import subprocess
import sys
from pathlib import Path

if __name__ == "__main__":
    # Run streamlit with the taxpayer search app
    taxpayer_app_path = Path(__file__).parent / "src" / "ui" / "taxpayer_search_app.py"
    
    cmd = [
        sys.executable,
        "-m",
        "streamlit",
        "run",
        str(taxpayer_app_path),
        "--server.port=8503",
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
