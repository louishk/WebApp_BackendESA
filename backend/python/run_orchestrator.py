#!/usr/bin/env python3
"""Orchestrator daemon entrypoint — used by systemd unit `backend-orchestrator`."""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

if __name__ == '__main__':
    from sync_service.daemon import main
    main()
