"""Compatibility entry point for the repository-owned streaming service."""
import sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT))
from scripts.demo.local_service import run

if __name__=='__main__':
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.parse_args()
    try:run('spark')
    except KeyboardInterrupt:print('Streaming stopped by user.')
