#!/usr/bin/env python3
"""
Vario Market Bot & Alarm Engine Runner
"""
import sys
from market_engine import execute_engine

if __name__ == "__main__":
    try:
        res = execute_engine()
        print(f"\n🎉 İŞLEM BAŞARIYLA TAMAMLANDI: {res}")
    except Exception as e:
        print(f"\n❌ ÇALIŞMA HATASI: {e}")
        sys.exit(1)
