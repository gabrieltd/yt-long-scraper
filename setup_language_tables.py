"""Setup script to initialize both English and Spanish database tables.

Run this script once to create both _es and _en table sets in your database.

Usage:
    python setup_language_tables.py
"""

import asyncio
import db
from dotenv import load_dotenv


async def setup_tables():
    """Initialize database tables for both languages."""
    load_dotenv()
    
    print("🔧 Setting up language-specific database tables...")
    
    # Create Spanish tables
    print("\n📊 Creating Spanish (_es) tables...")
    await db.init_db(language="es")
    await db.close_db()
    print("✅ Spanish tables created successfully!")
    
    # Reset pool and create English tables
    print("\n📊 Creating English (_en) tables...")
    await db.init_db(language="en")
    await db.close_db()
    print("✅ English tables created successfully!")
    
    print("\n🎉 Setup complete! Both language table sets are ready.")
    print("\nYou can now run:")
    print("  - Discovery with --EN flag for English data")
    print("  - Discovery with --ES flag for Spanish data")


if __name__ == "__main__":
    asyncio.run(setup_tables())
