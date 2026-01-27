#!/usr/bin/env python3
"""
Test script for bilingual YouTube scraper
Demonstrates different language and filter combinations
"""

import subprocess
import sys
from pathlib import Path

def run_test(description: str, command: list[str]):
    """Run a test command and report results"""
    print(f"\n{'='*60}")
    print(f"TEST: {description}")
    print(f"{'='*60}")
    print(f"Command: {' '.join(command)}\n")
    
    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=120  # 2 minute timeout
        )
        
        if result.returncode == 0:
            print("✅ SUCCESS")
            if result.stdout:
                print(f"Output:\n{result.stdout[:500]}")  # First 500 chars
        else:
            print("❌ FAILED")
            if result.stderr:
                print(f"Error:\n{result.stderr}")
                
    except subprocess.TimeoutExpired:
        print("⏱️ TIMEOUT - Test took too long")
    except Exception as e:
        print(f"💥 EXCEPTION: {e}")

def main():
    print("🧪 Bilingual YouTube Scraper Test Suite")
    print(f"Working directory: {Path.cwd()}")
    
    # Test 1: Spanish (default) with basic filters
    run_test(
        "Spanish (default) - Este mes + Más de 20 minutos",
        [
            sys.executable, "yt_discovery.py",
            "--query", "documental",
            "--limit", "5",
            "--upload-date", "this_month",
            "--duration", "over_20",
            "--headless"
        ]
    )
    
    # Test 2: English with filters
    run_test(
        "English - This month + Over 20 minutes",
        [
            sys.executable, "yt_discovery.py",
            "--query", "documentary",
            "--limit", "5",
            "--EN",
            "--upload-date", "this_month",
            "--duration", "over_20",
            "--headless"
        ]
    )
    
    # Test 3: English with multiple features
    run_test(
        "English - HD + Subtitles + Sort by view count",
        [
            sys.executable, "yt_discovery.py",
            "--query", "nature documentary",
            "--limit", "3",
            "--EN",
            "--features", "hd", "subtitles",
            "--sort-by", "view_count",
            "--headless"
        ]
    )
    
    # Test 4: Spanish with explicit flag
    run_test(
        "Spanish (explicit) - Esta semana + 4K",
        [
            sys.executable, "yt_discovery.py",
            "--query", "naturaleza",
            "--limit", "3",
            "--ES",
            "--upload-date", "this_week",
            "--features", "4k",
            "--headless"
        ]
    )
    
    # Test 5: Help text verification
    run_test(
        "Help text verification",
        [sys.executable, "yt_discovery.py", "--help"]
    )
    
    print("\n" + "="*60)
    print("🏁 Test suite completed")
    print("="*60)
    print("\nNOTE: These are smoke tests. Manual verification needed for:")
    print("  - Correct language locale in browser")
    print("  - Filter application in YouTube UI")
    print("  - Proper data parsing in both languages")
    print("\nRun with --headed flag to visually inspect browser behavior")

if __name__ == "__main__":
    main()
