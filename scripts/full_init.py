#!/usr/bin/env python3
"""
Full Initialization Script for PoE2 MCP Server
Integrates: environment setup, database creation, data population

Usage:
    python scripts/full_init.py              # Interactive mode
    python scripts/full_init.py --local      # Local JSON only
    python scripts/full_init.py --network    # Network scrape only
    python scripts/full_init.py --all        # Local + Network
    python scripts/full_init.py --reset      # Reset database first
"""

import asyncio
import sys
import io
import json
import argparse
from pathlib import Path
from datetime import datetime
from sqlalchemy import text

# Fix Windows encoding
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

# Add parent directory to path
BASE_DIR = Path(__file__).parent.parent.resolve()
sys.path.insert(0, str(BASE_DIR))


# ============================================================================
# Step 1: Environment Setup
# ============================================================================

def step1_environment() -> bool:
    """Check Python version and dependencies"""
    print("\n" + "=" * 60)
    print("Step 1: Environment Check")
    print("=" * 60)
    
    # Python version
    print("\n[1.1] Python version...")
    if sys.version_info < (3, 9):
        print(f"  [FAIL] Python 3.9+ required, you have {sys.version}")
        return False
    print(f"  [OK] Python {sys.version_info.major}.{sys.version_info.minor}")
    
    # Dependencies
    print("\n[1.2] Dependencies...")
    required = {
        'fastapi': 'fastapi',
        'uvicorn': 'uvicorn',
        'sqlalchemy': 'sqlalchemy',
        'httpx': 'httpx',
        'pydantic': 'pydantic',
        'pydantic_settings': 'pydantic_settings',
        'mcp': 'mcp',
        'bs4': 'beautifulsoup4',
    }
    
    missing = []
    for module, package in required.items():
        try:
            __import__(module)
            print(f"  [OK] {package}")
        except ImportError:
            print(f"  [MISSING] {package}")
            missing.append(package)
    
    if missing:
        print(f"\n  Run: pip install {' '.join(missing)}")
        return False
    
    return True


# ============================================================================
# Step 2: Directory & Config Setup
# ============================================================================

def step2_directories() -> bool:
    """Create directories and config files"""
    print("\n" + "=" * 60)
    print("Step 2: Directory & Config Setup")
    print("=" * 60)
    
    # Create directories
    print("\n[2.1] Creating directories...")
    dirs = ['data', 'cache', 'logs']
    for dir_name in dirs:
        dir_path = BASE_DIR / dir_name
        dir_path.mkdir(exist_ok=True)
        print(f"  [OK] {dir_name}/")
    
    # Check .env
    print("\n[2.2] Configuration file...")
    env_file = BASE_DIR / ".env"
    env_example = BASE_DIR / ".env.example"
    
    if not env_file.exists():
        if env_example.exists():
            import shutil
            shutil.copy(env_example, env_file)
            print("  [OK] Created .env from template")
        else:
            print("  [WARN] No .env.example found")
    else:
        print("  [OK] .env exists")
    
    return True


# ============================================================================
# Step 3: Database Setup
# ============================================================================

async def step3_database(reset: bool = False) -> bool:
    """Initialize database schema"""
    print("\n" + "=" * 60)
    print("Step 3: Database Setup")
    print("=" * 60)
    
    db_path = BASE_DIR / "data" / "poe2_optimizer.db"
    
    # Reset if requested
    if reset and db_path.exists():
        print("\n[3.1] Resetting database...")
        try:
            db_path.unlink()
            print("  [OK] Database deleted")
        except PermissionError:
            print("  [FAIL] Database locked by another process")
            return False
    
    # Initialize
    print("\n[3.2] Creating database schema...")
    try:
        from src.database.manager import DatabaseManager
        
        db = DatabaseManager()
        await db.initialize()
        
        # List tables
        async with db.async_session() as session:
            from sqlalchemy import text
            result = await session.execute(
                text("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            )
            tables = [row[0] for row in result.fetchall()]
        
        await db.close()
        print(f"  [OK] Created {len(tables)} tables: {', '.join(tables)}")
        return True
        
    except Exception as e:
        print(f"  [FAIL] {e}")
        return False


# ============================================================================
# Step 4: Data Population - Local JSON
# ============================================================================

async def step4_local_json() -> int:
    """Import data from local JSON files"""
    print("\n" + "=" * 60)
    print("Step 4: Local JSON Import")
    print("=" * 60)
    
    data_dir = BASE_DIR / "data"
    total = 0
    
    try:
        from src.database.manager import DatabaseManager
        from sqlalchemy import text
        
        db = DatabaseManager()
        await db.initialize()
        
        # Import spell gems
        spell_file = data_dir / "poe2_spell_gems_database.json"
        if spell_file.exists():
            count = await _import_spells(db, spell_file)
            print(f"\n[4.1] Spell gems: {count} imported")
            total += count
        else:
            print("\n[4.1] Spell gems: [SKIP] File not found")
        
        # Import support gems
        support_file = data_dir / "poe2_support_gems_database.json"
        if support_file.exists():
            count = await _import_supports(db, support_file)
            print(f"[4.2] Support gems: {count} imported")
            total += count
        else:
            print("[4.2] Support gems: [SKIP] File not found")
        
        # Import passive nodes
        passive_file = data_dir / "psg_passive_nodes.json"
        if passive_file.exists():
            count = await _import_passives(db, passive_file)
            print(f"[4.3] Passive nodes: {count} imported")
            total += count
        else:
            print("[4.3] Passive nodes: [SKIP] File not found")
        
        await db.close()
        return total
        
    except Exception as e:
        print(f"\n  [FAIL] {e}")
        import traceback
        traceback.print_exc()
        return 0


async def _import_spells(db, filepath: Path) -> int:
    """Import spell gems from nested JSON structure"""
    with open(filepath, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # Structure: {metadata: {...}, fire_spells: {name: {...}, ...}, ...}
    gems = []
    for key, value in data.items():
        if key == 'metadata':
            continue
        if isinstance(value, dict):
            for gem_name, gem_data in value.items():
                if isinstance(gem_data, dict) and 'name' in gem_data:
                    gems.append(gem_data)
    
    print(f"    Found {len(gems)} gems in JSON")
    
    count = 0
    errors = []
    async with db.async_session() as session:
        for gem in gems:
            try:
                await session.execute(
                    text("""
                        INSERT INTO skill_gems (name, gem_type, tags, required_level, primary_attribute, per_level_stats)
                        VALUES (:name, 'Active', :tags, :level, :attr, :stats)
                    """),
                    {
                        'name': gem.get('name', 'Unknown'),
                        'tags': json.dumps(gem.get('tags', [])),
                        'level': gem.get('level_requirement', gem.get('level', 1)),
                        'attr': gem.get('primary_attribute', gem.get('attribute', None)),
                        'stats': json.dumps(gem.get('per_level_stats', gem.get('stats', {})))
                    }
                )
                count += 1
            except Exception as e:
                errors.append(f"{gem.get('name', 'Unknown')}: {e}")
        await session.commit()
    
    if errors:
        print(f"    [WARN] {len(errors)} errors (first 3):")
        for err in errors[:3]:
            print(f"      - {err}")
    
    return count


async def _import_supports(db, filepath: Path) -> int:
    """Import support gems from nested JSON structure"""
    with open(filepath, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # Structure: {metadata: {...}, support_gems: {name: {...}, ...}, ...}
    gems = []
    for key in ['support_gems', 'lineage_support_gems', 'additional_support_gems_by_category']:
        if key in data and isinstance(data[key], dict):
            for gem_name, gem_data in data[key].items():
                if isinstance(gem_data, dict) and 'name' in gem_data:
                    gems.append(gem_data)
    
    print(f"    Found {len(gems)} supports in JSON")
    
    count = 0
    errors = []
    async with db.async_session() as session:
        for gem in gems:
            try:
                await session.execute(
                    text("""
                        INSERT INTO support_gems (name, tags, required_level, mana_multiplier, spirit_cost, modifiers, compatible_tags)
                        VALUES (:name, :tags, :level, :mana, :spirit, :mods, :compat)
                    """),
                    {
                        'name': gem.get('name', 'Unknown'),
                        'tags': json.dumps(gem.get('tags', [])),
                        'level': gem.get('level_requirement', gem.get('level', 1)),
                        'mana': gem.get('mana_multiplier', gem.get('mana_cost_multiplier', 100)),
                        'spirit': gem.get('spirit_cost', 0),
                        'mods': json.dumps(gem.get('modifiers', gem.get('effects', {}))),
                        'compat': json.dumps(gem.get('compatible_tags', gem.get('compatible_with', [])))
                    }
                )
                count += 1
            except Exception as e:
                errors.append(f"{gem.get('name', 'Unknown')}: {e}")
        await session.commit()
    
    if errors:
        print(f"    [WARN] {len(errors)} errors (first 3):")
        for err in errors[:3]:
            print(f"      - {err}")
    
    return count


async def _import_passives(db, filepath: Path) -> int:
    """Import passive nodes from JSON"""
    with open(filepath, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # Structure: {node_id: {...}, ...}
    nodes = []
    if isinstance(data, dict):
        for node_id, node_data in data.items():
            if isinstance(node_data, dict):
                node_data['id'] = node_id
                nodes.append(node_data)
    
    print(f"    Found {len(nodes)} nodes in JSON")
    
    count = 0
    errors = []
    async with db.async_session() as session:
        for node in nodes:
            try:
                await session.execute(
                    text("""
                        INSERT INTO passive_nodes (node_id, name, stats, position_x, position_y, is_keystone, is_notable)
                        VALUES (:id, :name, :stats, :x, :y, :keystone, :notable)
                    """),
                    {
                        'id': str(node.get('id', node.get('psg_id', count))),
                        'name': node.get('name', node.get('dn', f"Node {count}")),
                        'stats': json.dumps(node.get('stats', node.get('sd', []))),
                        'x': node.get('x', node.get('position_x', 0)),
                        'y': node.get('y', node.get('position_y', 0)),
                        'keystone': node.get('is_keystone', False),
                        'notable': node.get('is_notable', 'Notable' in node.get('name', ''))
                    }
                )
                count += 1
            except Exception as e:
                errors.append(f"{node.get('id', 'Unknown')}: {e}")
        await session.commit()
    
    if errors:
        print(f"    [WARN] {len(errors)} errors (first 3):")
        for err in errors[:3]:
            print(f"      - {err}")
    
    return count


# ============================================================================
# Step 5: Data Population - Network Scrape
# ============================================================================

async def step5_network_scrape() -> int:
    """Scrape data from network"""
    print("\n" + "=" * 60)
    print("Step 5: Network Scraping")
    print("=" * 60)
    print("\n  This may take several minutes...\n")
    
    try:
        from src.database.manager import DatabaseManager
        from src.utils.scraper import PoE2DataScraper
        from sqlalchemy import text
        
        db = DatabaseManager()
        await db.initialize()
        
        scraper = PoE2DataScraper()
        total = 0
        
        # Unique items
        print("[5.1] Scraping unique items...")
        items = await scraper.scrape_unique_items(limit=100)
        if items:
            async with db.async_session() as session:
                for item in items:
                    try:
                        await session.execute(
                            text("""
                                INSERT INTO items (name, base_type, item_class, rarity, level_requirement, data)
                                VALUES (:name, :base, :class, 'Unique', :level, :data)
                            """),
                            {
                                'name': item['name'],
                                'base': item.get('base_type', ''),
                                'class': item.get('item_class', 'Other'),
                                'level': item.get('level_requirement', 1),
                                'data': json.dumps(item)
                            }
                        )
                    except Exception:
                        continue
                await session.commit()
        print(f"      {len(items)} items imported")
        total += len(items)
        
        # Skill gems
        print("[5.2] Scraping skill gems...")
        skills = await scraper.scrape_skill_gems()
        if skills:
            async with db.async_session() as session:
                for skill in skills:
                    try:
                        await session.execute(
                            text("""
                                INSERT INTO skill_gems (name, gem_type, tags, data)
                                VALUES (:name, 'Active', :tags, :data)
                            """),
                            {
                                'name': skill['name'],
                                'tags': json.dumps(skill.get('tags', [])),
                                'data': json.dumps(skill)
                            }
                        )
                    except Exception:
                        continue
                await session.commit()
        print(f"      {len(skills)} skill gems imported")
        total += len(skills)
        
        # Support gems
        print("[5.3] Scraping support gems...")
        supports = await scraper.scrape_support_gems()
        if supports:
            async with db.async_session() as session:
                for support in supports:
                    try:
                        await session.execute(
                            text("""
                                INSERT INTO support_gems (name, tags, data)
                                VALUES (:name, :tags, :data)
                            """),
                            {
                                'name': support['name'],
                                'tags': json.dumps(support.get('tags', [])),
                                'data': json.dumps(support)
                            }
                        )
                    except Exception:
                        continue
                await session.commit()
        print(f"      {len(supports)} support gems imported")
        total += len(supports)
        
        await scraper.close()
        await db.close()
        
        return total
        
    except Exception as e:
        print(f"\n  [FAIL] {e}")
        import traceback
        traceback.print_exc()
        return 0


# ============================================================================
# Step 6: Verification
# ============================================================================

async def step6_verify() -> dict:
    """Verify database contents"""
    print("\n" + "=" * 60)
    print("Step 6: Verification")
    print("=" * 60)
    
    try:
        from src.database.manager import DatabaseManager
        from sqlalchemy import text
        
        db = DatabaseManager()
        await db.initialize()
        
        stats = {}
        tables = ['items', 'skill_gems', 'support_gems', 'passive_nodes']
        
        async with db.async_session() as session:
            for table in tables:
                try:
                    result = await session.execute(text(f"SELECT COUNT(*) FROM {table}"))
                    count = result.scalar()
                    stats[table] = count
                    status = "[OK]" if count > 0 else "[EMPTY]"
                    print(f"  {status} {table}: {count}")
                except Exception:
                    stats[table] = 0
                    print(f"  [ERROR] {table}")
        
        await db.close()
        return stats
        
    except Exception as e:
        print(f"  [FAIL] {e}")
        return {}


# ============================================================================
# Main
# ============================================================================

async def main(local: bool = False, network: bool = False, reset: bool = False):
    """Run full initialization"""
    
    print("\n" + "=" * 60)
    print("  PoE2 MCP Server - Full Initialization")
    print("=" * 60)
    
    # Step 1-2: Environment
    if not step1_environment():
        return False
    if not step2_directories():
        return False
    
    # Step 3: Database
    if not await step3_database(reset=reset):
        return False
    
    # Step 4-5: Data
    total = 0
    if local or network:
        if local:
            total += await step4_local_json()
        if network:
            total += await step5_network_scrape()
    else:
        # Interactive
        print("\n" + "-" * 60)
        print("Data source selection:")
        print("  [1] Local JSON files (fast, offline)")
        print("  [2] Network scraping (slow, latest data)")
        print("  [3] Both (local first, then network)")
        print("  [4] Skip data population")
        
        choice = input("\nSelect (1/2/3/4): ").strip()
        
        if choice == '1':
            total = await step4_local_json()
        elif choice == '2':
            total = await step5_network_scrape()
        elif choice == '3':
            total = await step4_local_json()
            total += await step5_network_scrape()
        else:
            print("\n[SKIP] Data population skipped")
    
    # Step 6: Verify
    stats = await step6_verify()
    
    # Summary
    print("\n" + "=" * 60)
    print("  Initialization Complete!")
    print("=" * 60)
    print(f"\n  Total records: {sum(stats.values())}")
    print("\n  Next steps:")
    print("    1. Reload CodeBuddy to start MCP")
    print("    2. Or run: python src/mcp_server.py")
    print()
    
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PoE2 MCP Full Initialization")
    parser.add_argument('--local', action='store_true', help='Import from local JSON')
    parser.add_argument('--network', action='store_true', help='Scrape from network')
    parser.add_argument('--all', action='store_true', help='Local + Network')
    parser.add_argument('--reset', action='store_true', help='Reset database first')
    
    args = parser.parse_args()
    
    try:
        success = asyncio.run(main(
            local=args.local or args.all,
            network=args.network or args.all,
            reset=args.reset
        ))
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\n[CANCELLED]")
        sys.exit(1)
