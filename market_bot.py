#!/usr/bin/env python3
"""
Vario Market Bot & Alarm Engine Runner
Supports modular runs:
  python market_bot.py --target crypto
  python market_bot.py --target bist
  python market_bot.py --target us
  python market_bot.py --target funds
  python market_bot.py --target all
"""
import sys
import argparse
from market_engine import execute_engine

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Vario Market & Alarm Engine")
    parser.add_argument(
        "--target",
        type=str,
        default="all",
        choices=["all", "crypto", "bist", "us", "funds", "fon"],
        help="Target market segment to fetch and evaluate (default: all)"
    )
    args = parser.parse_args()

    try:
        res = execute_engine(target=args.target)
        print(f"\n🎉 [{args.target.upper()}] İŞLEM BAŞARIYLA TAMAMLANDI: {res}")
    except Exception as e:
        print(f"\n❌ [{args.target.upper()}] ÇALIŞMA HATASI: {e}")
        sys.exit(1)
