"""Compatibility entry point for the repository-owned streaming service."""
import sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from scripts.local_service import run

if __name__=='__main__':
    try:run('spark')
    except KeyboardInterrupt:print('Streaming stopped by user.')
