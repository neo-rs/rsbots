#!/usr/bin/env python3
"""
Bot Inspector Module
--------------------
Auto-discovers and inspects RS bots without modifying their files.
Uses admin_bot.BOTS as canonical source of truth.
Scans configs, file sizes, folder structure, dependencies, and more.
"""

import os
import json
import stat
import re
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime
from collections import defaultdict


class BotInspector:
    """Inspect and discover RS bots without modifying their files.
    
    Uses admin_bot.BOTS dict as canonical source of truth.
    No hardcoded bot lists - derives everything from BOTS.
    """
    
    def __init__(self, project_root: Optional[Path] = None, bots_dict: Optional[Dict[str, Any]] = None):
        """
        Initialize bot inspector.
        
        Args:
            project_root: Root directory of the project (auto-detected if None)
            bots_dict: BOTS dictionary from admin_bot (canonical source). If None, will try to import.
        """
        if project_root is None:
            # Auto-detect project root (look for RSAdminBot folder)
            current = Path(__file__).parent
            if current.name == "RSAdminBot":
                project_root = current.parent
            else:
                project_root = current
        
        self.project_root = Path(project_root).resolve()
        self.bots: Dict[str, Dict[str, Any]] = {}
        
        # Load BOTS dict from admin_bot (canonical source)
        if bots_dict is None:
            try:
                import sys
                admin_bot_path = self.project_root / "RSAdminBot" / "admin_bot.py"
                if admin_bot_path.exists():
                    # Import admin_bot to get BOTS dict (class attribute)
                    if str(self.project_root / "RSAdminBot") not in sys.path:
                        sys.path.insert(0, str(self.project_root / "RSAdminBot"))
                    from admin_bot import RSAdminBot
                    # BOTS is a class attribute, access directly
                    self.bots_dict = RSAdminBot.BOTS if hasattr(RSAdminBot, 'BOTS') else {}
                else:
                    self.bots_dict = {}
            except Exception:
                self.bots_dict = {}
        else:
            self.bots_dict = bots_dict
    
    def discover_bots(self) -> Dict[str, Dict[str, Any]]:
        """
        Auto-discover all RS bots from canonical BOTS dict.
        
        Returns:
            Dictionary of discovered bots with their info
        """
        self.bots = {}
        
        if not self.bots_dict:
            return self.bots
        
        # Use BOTS dict as canonical source (no hardcoded lists)
        for bot_key, bot_info in self.bots_dict.items():
            folder_name = bot_info.get("folder", "")
            if not folder_name:
                continue
            
            bot_path = self.project_root / folder_name
            
            if not bot_path.exists() or not bot_path.is_dir():
                continue
            
            # Inspect using canonical info from BOTS dict
            inspected_info = self._inspect_bot_folder(bot_key, bot_info, bot_path)
            if inspected_info:
                self.bots[bot_key] = inspected_info
        
        return self.bots
    
    def _inspect_bot_folder(self, bot_key: str, bot_info_canonical: Dict[str, Any], folder_path: Path) -> Optional[Dict[str, Any]]:
        """Inspect a single bot folder and extract comprehensive information.
        
        Args:
            bot_key: Bot key from BOTS dict (e.g., "rssuccessbot")
            bot_info_canonical: Canonical bot info from admin_bot.BOTS dict
            folder_path: Path to bot folder
        """
        try:
            folder_name = bot_info_canonical.get("folder", bot_key)
            script_name = bot_info_canonical.get("script", None)
            service_name = bot_info_canonical.get("service", f"mirror-world-{bot_key}.service")
            
            bot_info = {
                "key": bot_key,
                "name": bot_info_canonical.get("name", folder_name),
                "folder": folder_name,
                "path": str(folder_path),
                "exists": True,
                "config": {},
                "config_valid": False,
                "files": {},
                "structure": {},
                "size": 0,
                "script": None,
                "script_exists": False,
                "script_path": None,
                "service": service_name,
                "dependencies": {},
                "python_files": [],
                "has_requirements": False,
                "requirements_count": 0,
                "has_readme": False,
                "file_count": 0,
                "last_modified": None,
                "health": {}
            }
            
            # Check if main script exists (from canonical source)
            if script_name:
                script_path = folder_path / script_name
                if script_path.exists():
                    bot_info["script"] = script_name
                    bot_info["script_exists"] = True
                    bot_info["script_path"] = str(script_path)
                    # Check if script is executable
                    try:
                        bot_info["script_executable"] = os.access(script_path, os.X_OK)
                    except:
                        bot_info["script_executable"] = False
                else:
                    # Try to find script if canonical name doesn't exist
                    bot_info["script_not_found"] = script_name
            
            # Read config.json
            config_path = folder_path / "config.json"
            if config_path.exists():
                try:
                    with open(config_path, 'r', encoding='utf-8') as f:
                        config_data = json.load(f)
                        bot_info["config"] = config_data
                        bot_info["config_valid"] = True
                        # Check for common required fields
                        # Tokens are server-only in config.secrets.json (do not require in config.json)
                        bot_info["has_bot_token"] = False
                        bot_info["config_keys"] = list(config_data.keys())
                except Exception as e:
                    bot_info["config_error"] = str(e)
                    bot_info["config_valid"] = False

            # Read config.secrets.json presence (never include secret values)
            secrets_path = folder_path / "config.secrets.json"
            bot_info["has_secrets_file"] = secrets_path.exists()
            bot_info["has_bot_token"] = False
            if secrets_path.exists():
                try:
                    with open(secrets_path, 'r', encoding='utf-8') as f:
                        secrets_data = json.load(f)
                    if isinstance(secrets_data, dict):
                        bot_info["has_bot_token"] = bool(secrets_data.get("bot_token"))
                        bot_info["secrets_keys"] = list(secrets_data.keys())[:25]
                except Exception as e:
                    bot_info["secrets_error"] = str(e)
            
            # Check for requirements.txt
            requirements_path = folder_path / "requirements.txt"
            if requirements_path.exists():
                bot_info["has_requirements"] = True
                try:
                    with open(requirements_path, 'r', encoding='utf-8') as f:
                        requirements = [line.strip() for line in f if line.strip() and not line.startswith('#')]
                        bot_info["requirements_count"] = len(requirements)
                        bot_info["dependencies"]["requirements"] = requirements[:20]  # First 20
                except Exception as e:
                    bot_info["requirements_error"] = str(e)
            
            # Check for README
            readme_paths = [folder_path / "README.md", folder_path / "readme.md", folder_path / "Readme.md"]
            for readme_path in readme_paths:
                if readme_path.exists():
                    bot_info["has_readme"] = True
                    bot_info["readme_path"] = str(readme_path)
                    break
            
            # Scan Python files
            python_files = []
            try:
                for root, dirs, files in os.walk(folder_path):
                    dirs[:] = [d for d in dirs if d not in ['__pycache__', '.git', 'node_modules', '.venv', 'venv']]
                    for file in files:
                        if file.endswith('.py'):
                            file_path = Path(root) / file
                            rel_path = file_path.relative_to(folder_path)
                            python_files.append(str(rel_path))
            except Exception:
                pass
            
            bot_info["python_files"] = python_files[:50]  # First 50
            bot_info["python_file_count"] = len(python_files)
            
            # Scan files and structure
            bot_info["files"], bot_info["size"] = self._scan_folder(folder_path)
            bot_info["structure"] = self._get_folder_structure(folder_path)
            
            # Get file count
            bot_info["file_count"] = len(bot_info["files"])
            
            # Get last modified (most recent file)
            bot_info["last_modified"] = self._get_last_modified(folder_path)
            bot_info["last_modified_file"] = self._get_most_recent_file(folder_path)
            
            # Calculate health score
            bot_info["health"] = self._calculate_health(bot_info)
            
            return bot_info
            
        except Exception as e:
            return {
                "key": bot_key,
                "name": bot_info_canonical.get("name", bot_key),
                "folder": bot_info_canonical.get("folder", bot_key),
                "path": str(folder_path),
                "exists": True,
                "error": str(e)
            }
    
    def _scan_folder(self, folder_path: Path, max_depth: int = 10) -> Tuple[Dict[str, int], int]:
        """
        Scan folder and get file sizes.
        
        Returns:
            Tuple of (file_sizes_dict, total_size_bytes)
        """
        file_sizes = {}
        total_size = 0
        
        try:
            for root, dirs, files in os.walk(folder_path):
                # Skip common ignore patterns
                dirs[:] = [d for d in dirs if d not in ['__pycache__', '.git', 'node_modules', '.venv']]
                
                depth = root.replace(str(folder_path), '').count(os.sep)
                if depth > max_depth:
                    continue
                
                for file in files:
                    file_path = Path(root) / file
                    try:
                        if file_path.is_file():
                            size = file_path.stat().st_size
                            rel_path = file_path.relative_to(folder_path)
                            file_sizes[str(rel_path)] = size
                            total_size += size
                    except (OSError, PermissionError):
                        pass
        except Exception:
            pass
        
        return file_sizes, total_size
    
    def _get_folder_structure(self, folder_path: Path, max_depth: int = 3) -> Dict[str, Any]:
        """Get folder structure as a tree"""
        structure = {}
        
        try:
            for item in sorted(folder_path.iterdir()):
                if item.name.startswith('.'):
                    continue
                
                if item.is_dir():
                    if item.name in ['__pycache__', '.git', 'node_modules', '.venv']:
                        continue
                    
                    if max_depth > 0:
                        structure[item.name + '/'] = self._get_folder_structure(item, max_depth - 1)
                    else:
                        structure[item.name + '/'] = {}
                else:
                    structure[item.name] = item.stat().st_size
        except (OSError, PermissionError):
            pass
        
        return structure
    
    def _get_last_modified(self, folder_path: Path) -> Optional[str]:
        """Get last modified time of folder"""
        try:
            mtime = os.path.getmtime(folder_path)
            return datetime.fromtimestamp(mtime).isoformat()
        except Exception:
            return None
    
    def _get_most_recent_file(self, folder_path: Path) -> Optional[Dict[str, Any]]:
        """Get the most recently modified file in the folder"""
        most_recent = None
        most_recent_time = 0
        
        try:
            for root, dirs, files in os.walk(folder_path):
                dirs[:] = [d for d in dirs if d not in ['__pycache__', '.git', 'node_modules', '.venv', 'venv']]
                for file in files:
                    file_path = Path(root) / file
                    try:
                        mtime = os.path.getmtime(file_path)
                        if mtime > most_recent_time:
                            most_recent_time = mtime
                            rel_path = file_path.relative_to(folder_path)
                            most_recent = {
                                "file": str(rel_path),
                                "modified": datetime.fromtimestamp(mtime).isoformat()
                            }
                    except (OSError, PermissionError):
                        pass
        except Exception:
            pass
        
        return most_recent
    
    def _calculate_health(self, bot_info: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate health score for a bot"""
        health = {
            "score": 0,
            "max_score": 100,
            "checks": {}
        }
        
        # Config exists and valid (+20)
        if bot_info.get("config_valid"):
            health["score"] += 20
            health["checks"]["config_valid"] = True
        else:
            health["checks"]["config_valid"] = False
        
        # Script exists (+20)
        if bot_info.get("script_exists"):
            health["score"] += 20
            health["checks"]["script_exists"] = True
        else:
            health["checks"]["script_exists"] = False
        
        # Has bot token (in server-only secrets) (+15)
        if bot_info.get("has_bot_token"):
            health["score"] += 15
            health["checks"]["has_bot_token"] = True
        else:
            health["checks"]["has_bot_token"] = False
        
        # Has requirements.txt (+10)
        if bot_info.get("has_requirements"):
            health["score"] += 10
            health["checks"]["has_requirements"] = True
        else:
            health["checks"]["has_requirements"] = False
        
        # Has README (+10)
        if bot_info.get("has_readme"):
            health["score"] += 10
            health["checks"]["has_readme"] = True
        else:
            health["checks"]["has_readme"] = False
        
        # Has Python files (+10)
        if bot_info.get("python_file_count", 0) > 0:
            health["score"] += 10
            health["checks"]["has_python_files"] = True
        else:
            health["checks"]["has_python_files"] = False
        
        # Recent activity (+5)
        last_modified = bot_info.get("last_modified")
        if last_modified:
            try:
                mod_time = datetime.fromisoformat(last_modified.replace('Z', '+00:00'))
                days_ago = (datetime.now(mod_time.tzinfo) - mod_time).days
                if days_ago < 30:
                    health["score"] += 5
                    health["checks"]["recent_activity"] = True
                else:
                    health["checks"]["recent_activity"] = False
            except:
                health["checks"]["recent_activity"] = None
        
        # Determine status
        if health["score"] >= 80:
            health["status"] = "excellent"
        elif health["score"] >= 60:
            health["status"] = "good"
        elif health["score"] >= 40:
            health["status"] = "fair"
        else:
            health["status"] = "poor"
        
        return health
    
    def get_bot_info(self, bot_name: str) -> Optional[Dict[str, Any]]:
        """Get detailed info for a specific bot"""
        if not self.bots:
            self.discover_bots()
        
        bot_name_lower = bot_name.lower()
        return self.bots.get(bot_name_lower)
    
    def get_bot_config(self, bot_name: str) -> Optional[Dict[str, Any]]:
        """Get config.json for a specific bot"""
        bot_info = self.get_bot_info(bot_name)
        if bot_info:
            return bot_info.get("config", {})
        return None
    
    def get_bot_size(self, bot_name: str) -> Tuple[int, str]:
        """
        Get bot folder size.
        
        Returns:
            Tuple of (size_bytes, formatted_size)
        """
        bot_info = self.get_bot_info(bot_name)
        if bot_info:
            size_bytes = bot_info.get("size", 0)
            return size_bytes, self._format_size(size_bytes)
        return 0, "0 B"
    
    def _format_size(self, size_bytes: int) -> str:
        """Format bytes to human-readable size"""
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size_bytes < 1024.0:
                return f"{size_bytes:.2f} {unit}"
            size_bytes /= 1024.0
        return f"{size_bytes:.2f} TB"
    
    def get_all_bots_summary(self) -> List[Dict[str, Any]]:
        """Get enhanced summary of all discovered bots"""
        if not self.bots:
            self.discover_bots()
        
        summary = []
        for bot_key, bot_info in self.bots.items():
            health = bot_info.get("health", {})
            summary.append({
                "key": bot_key,
                "name": bot_info.get("name", "Unknown"),
                "folder": bot_info.get("folder", "Unknown"),
                "script": bot_info.get("script", "Unknown"),
                "script_exists": bot_info.get("script_exists", False),
                "size": self._format_size(bot_info.get("size", 0)),
                "file_count": bot_info.get("file_count", 0),
                "python_file_count": bot_info.get("python_file_count", 0),
                "has_config": bot_info.get("config_valid", False),
                "has_bot_token": bot_info.get("has_bot_token", False),
                "has_requirements": bot_info.get("has_requirements", False),
                "requirements_count": bot_info.get("requirements_count", 0),
                "has_readme": bot_info.get("has_readme", False),
                "last_modified": bot_info.get("last_modified", "Unknown"),
                "health_score": health.get("score", 0),
                "health_status": health.get("status", "unknown")
            })
        
        return summary
    
    def compare_bots(self, bot1_name: str, bot2_name: str) -> Dict[str, Any]:
        """Compare two bots"""
        bot1_info = self.get_bot_info(bot1_name)
        bot2_info = self.get_bot_info(bot2_name)
        
        if not bot1_info or not bot2_info:
            return {"error": "One or both bots not found"}
        
        return {
            "bot1": {
                "name": bot1_info.get("name"),
                "size": self._format_size(bot1_info.get("size", 0)),
                "file_count": bot1_info.get("file_count", 0)
            },
            "bot2": {
                "name": bot2_info.get("name"),
                "size": self._format_size(bot2_info.get("size", 0)),
                "file_count": bot2_info.get("file_count", 0)
            },
            "differences": {
                "size_diff": bot1_info.get("size", 0) - bot2_info.get("size", 0),
                "file_count_diff": bot1_info.get("file_count", 0) - bot2_info.get("file_count", 0)
            }
        }
    
    def generate_report(self) -> str:
        """Generate a text report of all bots"""
        if not self.bots:
            self.discover_bots()
        
        report = []
        report.append("=" * 80)
        report.append("RS Bots Inspection Report")
        report.append(f"Generated: {datetime.now().isoformat()}")
        report.append(f"Project Root: {self.project_root}")
        report.append("=" * 80)
        report.append("")
        
        for bot_key, bot_info in sorted(self.bots.items()):
            report.append(f"Bot: {bot_info.get('name', 'Unknown')}")
            report.append(f"  Folder: {bot_info.get('folder', 'Unknown')}")
            report.append(f"  Script: {bot_info.get('script', 'Unknown')}")
            report.append(f"  Service: {bot_info.get('service', 'Unknown')}")
            report.append(f"  Size: {self._format_size(bot_info.get('size', 0))}")
            report.append(f"  Files: {bot_info.get('file_count', 0)}")
            report.append(f"  Last Modified: {bot_info.get('last_modified', 'Unknown')}")
            report.append(f"  Has Config: {'Yes' if bot_info.get('config') else 'No'}")
            report.append("")
        
        return "\n".join(report)


# Convenience functions
def discover_all_bots(project_root: Optional[Path] = None) -> Dict[str, Dict[str, Any]]:
    """Discover all bots"""
    inspector = BotInspector(project_root)
    return inspector.discover_bots()


def get_bot_info(bot_name: str, project_root: Optional[Path] = None) -> Optional[Dict[str, Any]]:
    """Get info for a specific bot"""
    inspector = BotInspector(project_root)
    return inspector.get_bot_info(bot_name)


def get_bot_config(bot_name: str, project_root: Optional[Path] = None) -> Optional[Dict[str, Any]]:
    """Get config for a specific bot"""
    inspector = BotInspector(project_root)
    return inspector.get_bot_config(bot_name)

