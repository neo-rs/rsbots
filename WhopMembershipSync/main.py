#!/usr/bin/env python3
"""
Main entry point for Whop Membership to Google Sheets sync.
"""

import asyncio
import json
import logging
import sys
from pathlib import Path
from typing import Dict, Any

# Setup logging with more detail
logging.basicConfig(
    level=logging.DEBUG,  # Changed to DEBUG for more verbose output
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger("whop-sheets-sync")

# Suppress RSCheckerbot's verbose warnings (we handle errors ourselves)
logging.getLogger("rs-checker").setLevel(logging.ERROR)  # Only show errors, not warnings

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from whop_sheets_sync import WhopSheetsSync
from RSCheckerbot.whop_api_client import WhopAPIClient, WhopAPIError


def load_config() -> Dict[str, Any]:
    """Load configuration from config.json and config.secrets.json."""
    config_dir = Path(__file__).parent
    config_file = config_dir / "config.json"
    secrets_file = config_dir / "config.secrets.json"
    
    cfg = {}
    if config_file.exists():
        try:
            cfg = json.loads(config_file.read_text(encoding="utf-8"))
        except Exception as e:
            log.error(f"Failed to load config.json: {e}")
            sys.exit(1)
    else:
        log.error("config.json not found")
        sys.exit(1)
    
    # Load secrets
    if secrets_file.exists():
        try:
            secrets = json.loads(secrets_file.read_text(encoding="utf-8"))
            # Merge secrets into config
            if "whop_api" in secrets:
                if "whop_api" not in cfg:
                    cfg["whop_api"] = {}
                cfg["whop_api"].update(secrets["whop_api"])
            if "google_service_account_json" in secrets:
                cfg["google_service_account_json"] = secrets["google_service_account_json"]
        except Exception as e:
            log.warning(f"Failed to load config.secrets.json: {e}")
    
    # Try to load Google service account from RSForwarder if not in secrets
    if "google_service_account_json" not in cfg or not cfg.get("google_service_account_json"):
        rsforwarder_secrets = Path(__file__).resolve().parents[1] / "RSForwarder" / "config.secrets.json"
        if rsforwarder_secrets.exists():
            try:
                rs_secrets = json.loads(rsforwarder_secrets.read_text(encoding="utf-8"))
                if "google_service_account_json" in rs_secrets:
                    cfg["google_service_account_json"] = rs_secrets["google_service_account_json"]
                    log.info("Loaded Google service account from RSForwarder/config.secrets.json")
            except Exception as e:
                log.warning(f"Failed to load RSForwarder secrets: {e}")
    
    return cfg


async def run_sync_once(cfg: Dict[str, Any]) -> None:
    """Run a single sync cycle."""
    # Initialize Whop API client
    log.info("Initializing Whop API client...")
    whop_cfg = cfg.get("whop_api", {})
    api_key = whop_cfg.get("api_key", "").strip()
    base_url = whop_cfg.get("base_url", "https://api.whop.com/api/v1").strip()
    company_id = whop_cfg.get("company_id", "").strip()
    
    if not api_key:
        log.error("✗ Missing whop_api.api_key in config.secrets.json")
        print("X ERROR: Missing whop_api.api_key in config.secrets.json")
        return
    
    if not company_id:
        log.error("✗ Missing whop_api.company_id in config.json")
        print("X ERROR: Missing whop_api.company_id in config.json")
        return
    
    log.debug(f"  API Key: {'*' * 20}...{api_key[-4:] if len(api_key) > 4 else '****'}")
    log.debug(f"  Base URL: {base_url}")
    log.debug(f"  Company ID: {company_id}")
    
    try:
        whop_client = WhopAPIClient(api_key=api_key, base_url=base_url, company_id=company_id)
        log.info("✓ Whop API client initialized successfully")
        print("OK Whop API client initialized successfully")
    except ValueError as e:
        log.error(f"✗ Invalid configuration: {e}", exc_info=True)
        print(f"X ERROR: Invalid configuration: {e}")
        return
    except Exception as e:
        log.error(f"✗ Failed to initialize Whop API client: {type(e).__name__}: {e}", exc_info=True)
        print(f"X ERROR: Failed to initialize Whop API client: {type(e).__name__}: {e}")
        return
    
    # Initialize Sheets sync
    log.info("Initializing Google Sheets sync...")
    print("Initializing Google Sheets sync...")
    try:
        sheets_sync = WhopSheetsSync(cfg)
        log.info("✓ Google Sheets sync initialized")
        print("OK Google Sheets sync initialized")
    except Exception as e:
        log.error(f"✗ Failed to initialize Google Sheets sync: {type(e).__name__}: {e}", exc_info=True)
        print(f"X ERROR: Failed to initialize Google Sheets sync: {type(e).__name__}: {e}")
        return
    
    # Sync all products
    log.info("=" * 60)
    log.info("Starting Whop membership sync...")
    log.info("=" * 60)
    print("=" * 60)
    print("Starting Whop membership sync...")
    print("=" * 60)
    results = await sheets_sync.sync_all_products(whop_client)
    
    # Print summary
    log.info("")
    log.info("=" * 60)
    log.info("=== Sync Summary ===")
    log.info("=" * 60)
    print("")
    print("=" * 60)
    print("=== Sync Summary ===")
    print("=" * 60)
    
    for product_id, (success, msg, count) in results.items():
        status = "OK" if success else "X"
        status_icon = "OK" if success else "X"
        log.info(f"{status} {product_id}: {count} members - {msg}")
        print(f"{status_icon} {product_id}: {count} members - {msg}")
    
    total_success = sum(1 for s, _, _ in results.values() if s)
    total_members = sum(count for _, _, count in results.values())
    
    log.info("")
    log.info(f"Total: {total_success}/{len(results)} products synced, {total_members} total members")
    print("")
    print(f"Total: {total_success}/{len(results)} products synced, {total_members} total members")
    print("=" * 60)


async def main():
    """Main sync function."""
    cfg = load_config()
    
    if not cfg.get("enabled", True):
        log.info("Sync is disabled in config")
        return
    
    # Check if continuous sync is enabled
    continuous_cfg = cfg.get("continuous_sync", {})
    if continuous_cfg.get("enabled", False):
        interval_minutes = continuous_cfg.get("check_interval_minutes", 15)
        log.info(f"Continuous sync enabled - checking every {interval_minutes} minutes")
        print(f"Continuous sync enabled - checking every {interval_minutes} minutes")
        print("Press Ctrl+C to stop")
        
        # First run: full sync
        log.info("=" * 60)
        log.info("Initial sync (startup)...")
        log.info("=" * 60)
        print("=" * 60)
        print("Initial sync (startup)...")
        print("=" * 60)
        await run_sync_once(cfg)
        
        # Then run continuous cycles
        cycle_count = 0
        try:
            while True:
                cycle_count += 1
                log.info(f"\n{'='*60}")
                log.info(f"Continuous sync cycle #{cycle_count}")
                log.info(f"{'='*60}")
                print(f"\n{'='*60}")
                print(f"Continuous sync cycle #{cycle_count}")
                print(f"{'='*60}")
                
                log.info(f"Waiting {interval_minutes} minutes until next sync...")
                print(f"Waiting {interval_minutes} minutes until next sync...")
                await asyncio.sleep(interval_minutes * 60)
                
                # Continuous cycle: update source, then segregate
                try:
                    await run_continuous_cycle(cfg)
                    log.info(f"✓ Cycle #{cycle_count} completed successfully")
                    print(f"OK Cycle #{cycle_count} completed successfully")
                except Exception as e:
                    log.error(f"✗ Cycle #{cycle_count} failed: {e}", exc_info=True)
                    print(f"X Cycle #{cycle_count} failed: {e}")
                    # Continue to next cycle even if this one failed
                    continue
        except KeyboardInterrupt:
            log.info("Continuous sync stopped by user")
            print("\nContinuous sync stopped")
        except Exception as e:
            log.error(f"Continuous sync crashed: {e}", exc_info=True)
            print(f"\nX Continuous sync crashed: {e}")
            raise
    else:
        # Run once (startup sync)
        await run_sync_once(cfg)


async def run_continuous_cycle(cfg: Dict[str, Any]) -> None:
    """Run a continuous sync cycle: update source tab, then segregate by status."""
    # Initialize Whop API client
    whop_cfg = cfg.get("whop_api", {})
    api_key = whop_cfg.get("api_key", "").strip()
    base_url = whop_cfg.get("base_url", "https://api.whop.com/api/v1").strip()
    company_id = whop_cfg.get("company_id", "").strip()
    
    if not api_key or not company_id:
        log.error("Missing Whop API configuration")
        return
    
    try:
        whop_client = WhopAPIClient(api_key=api_key, base_url=base_url, company_id=company_id)
        sheets_sync = WhopSheetsSync(cfg)
        
        # Run continuous cycle
        results = await sheets_sync.sync_continuous_cycle(whop_client)
        
        # Print summary
        log.info("")
        log.info("=== Continuous Sync Summary ===")
        print("")
        print("=== Continuous Sync Summary ===")
        
        for tab_name, (success, msg, count) in results.items():
            status_icon = "OK" if success else "X"
            log.info(f"{status_icon} {tab_name}: {count} members - {msg}")
            print(f"{status_icon} {tab_name}: {count} members - {msg}")
        
    except Exception as e:
        log.error(f"Continuous cycle failed: {e}", exc_info=True)
        print(f"X Continuous cycle failed: {e}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Interrupted by user")
        sys.exit(0)
    except Exception as e:
        log.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)
