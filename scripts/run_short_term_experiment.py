"""Run offline ML selection first, evaluate the sealed run separately."""
import argparse
import sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from ml.short_term_experiment import select,evaluate

if __name__=="__main__":
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase",choices=["select","evaluate"])
    parser.add_argument("--run",type=Path)
    args=parser.parse_args()
    if args.phase=="evaluate":
        if args.run is None:parser.error("evaluate requires --run")
        evaluate(args.run)
    else:print(select(args.run))
