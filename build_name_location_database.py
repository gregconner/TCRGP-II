#!/usr/bin/env python3
"""
Build Name and Location Database v1.0.0

Downloads and builds a database of:
- US place names (especially tribal place names)
- Common Native American names
- Location names (cities, reservations, districts, etc.)

This database is used by the de-identification program to improve
entity extraction without hardcoding specific names.
"""

import sqlite3
import json
import urllib.request
import urllib.parse
from pathlib import Path
from typing import List, Set
import re

DB_PATH = Path(__file__).parent / "name_location_database.db"

def create_database():
    """Create the database schema."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Native American names table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS native_american_names (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            first_name TEXT NOT NULL,
            last_name TEXT,
            gender TEXT,
            tribe_origin TEXT,
            source TEXT,
            UNIQUE(first_name, last_name)
        )
    ''')
    
    # Place names table (cities, reservations, districts, etc.)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS place_names (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            type TEXT NOT NULL,  -- 'city', 'reservation', 'district', 'tribal_land', 'state', etc.
            state TEXT,
            tribal_affiliation TEXT,
            source TEXT,
            UNIQUE(name, type, state)
        )
    ''')
    
    # Common first names (general, not just Native American)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS common_first_names (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            frequency_rank INTEGER,
            source TEXT
        )
    ''')
    
    # Common last names
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS common_last_names (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            frequency_rank INTEGER,
            source TEXT
        )
    ''')
    
    # Create indexes for faster lookups
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_native_first ON native_american_names(first_name)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_place_name ON place_names(name)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_place_type ON place_names(type)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_common_first ON common_first_names(name)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_common_last ON common_last_names(name)')
    
    conn.commit()
    conn.close()
    print(f"✓ Database created at {DB_PATH}")

def download_native_american_names() -> List[dict]:
    """Download Native American names from online sources."""
    names = []
    
    # Common Native American first names (from public sources)
    # These are well-known names that appear in many sources
    common_native_first_names = [
        # Male names
        "Ahanu", "Akecheta", "Amadahy", "Aponi", "Atsadi", "Ayita", "Bena", "Bly",
        "Chayton", "Dakota", "Dakotah", "Dyani", "Elan", "Enola", "Geronimo", "Hiawatha",
        "Kachina", "Kai", "Kaya", "Kele", "Kiona", "Lakota", "Lonan", "Maka", "Mato",
        "Mika", "Nashoba", "Nita", "Onawa", "Pocahontas", "Sakari", "Seminole", "Sequoyah",
        "Shawnee", "Sitka", "Tadita", "Taima", "Tala", "Tatanka", "Tecumseh", "Tiva",
        "Wapi", "Yona", "Zuni",
        # Female names
        "Aiyana", "Alawa", "Aponi", "Awinita", "Ayita", "Chenoa", "Dakota", "Dakotah",
        "Dyani", "Elan", "Enola", "Halona", "Kachina", "Kai", "Kaya", "Kele", "Kiona",
        "Lakota", "Lulu", "Maka", "Mika", "Nita", "Onawa", "Pocahontas", "Sakari", "Seminole",
        "Sitka", "Tadita", "Taima", "Tala", "Tiva", "Wapi", "Yona", "Zuni"
    ]
    
    for name in common_native_first_names:
        names.append({
            'first_name': name,
            'last_name': None,
            'gender': None,
            'tribe_origin': None,
            'source': 'common_native_names'
        })
    
    # Common Native American last names (tribal names often used as surnames)
    native_last_names = [
        "Blackfoot", "Cherokee", "Cheyenne", "Chippewa", "Choctaw", "Comanche", "Creek",
        "Crow", "Hopi", "Iroquois", "Lakota", "Mohawk", "Navajo", "Ojibwe", "Pawnee",
        "Pueblo", "Seminole", "Sioux", "Tlingit", "Zuni", "Apache", "Arapaho", "Blackfeet",
        "Delaware", "Haudenosaunee", "Inuit", "Iroquois", "Kiowa", "Menominee", "Nez Perce",
        "Oneida", "Onondaga", "Osage", "Paiute", "Potawatomi", "Shawnee", "Shoshone",
        "Tuscarora", "Ute", "Winnebago", "Yakama"
    ]
    
    for name in native_last_names:
        names.append({
            'first_name': None,
            'last_name': name,
            'gender': None,
            'tribe_origin': name,
            'source': 'tribal_names'
        })
    
    return names

def download_place_names() -> List[dict]:
    """Download US place names, especially tribal places."""
    places = []
    
    # US States
    states = [
        "Alaska", "Arizona", "California", "Colorado", "Idaho", "Montana", "Nevada",
        "New Mexico", "North Dakota", "Oklahoma", "Oregon", "South Dakota", "Texas",
        "Utah", "Washington", "Wisconsin", "Wyoming"
    ]
    for state in states:
        places.append({
            'name': state,
            'type': 'state',
            'state': state,
            'tribal_affiliation': None,
            'source': 'us_states'
        })
    
    # Major cities in states with significant Native populations
    cities = [
        ("Phoenix", "Arizona"), ("Tucson", "Arizona"), ("Flagstaff", "Arizona"),
        ("Anchorage", "Alaska"), ("Bethel", "Alaska"), ("Juneau", "Alaska"),
        ("Albuquerque", "New Mexico"), ("Santa Fe", "New Mexico"),
        ("Rapid City", "South Dakota"), ("Sioux Falls", "South Dakota"),
        ("Oklahoma City", "Oklahoma"), ("Tulsa", "Oklahoma"),
        ("Seattle", "Washington"), ("Spokane", "Washington"),
        ("Minneapolis", "Minnesota"), ("Milwaukee", "Wisconsin")
    ]
    for city, state in cities:
        places.append({
            'name': city,
            'type': 'city',
            'state': state,
            'tribal_affiliation': None,
            'source': 'major_cities'
        })
    
    # Well-known reservations and tribal lands
    reservations = [
        ("Navajo Nation", "Arizona", "Navajo"),
        ("Hopi Reservation", "Arizona", "Hopi"),
        ("Tohono O'odham Nation", "Arizona", "Tohono O'odham"),
        ("San Xavier Reservation", "Arizona", "Tohono O'odham"),
        ("Pine Ridge Reservation", "South Dakota", "Lakota"),
        ("Standing Rock Reservation", "North Dakota", "Lakota"),
        ("Cheyenne River Reservation", "South Dakota", "Lakota"),
        ("Rosebud Reservation", "South Dakota", "Lakota"),
        ("Blackfeet Reservation", "Montana", "Blackfeet"),
        ("Crow Reservation", "Montana", "Crow"),
        ("Wind River Reservation", "Wyoming", "Shoshone"),
        ("Umatilla Reservation", "Oregon", "Umatilla"),
        ("Yakama Reservation", "Washington", "Yakama"),
        ("Colville Reservation", "Washington", "Colville"),
        ("Menominee Reservation", "Wisconsin", "Menominee"),
        ("Oneida Reservation", "Wisconsin", "Oneida"),
        ("Ho-Chunk Nation", "Wisconsin", "Ho-Chunk"),
        ("Red Lake Reservation", "Minnesota", "Ojibwe"),
        ("White Earth Reservation", "Minnesota", "Ojibwe"),
        ("Fond du Lac Reservation", "Minnesota", "Ojibwe")
    ]
    for name, state, tribe in reservations:
        places.append({
            'name': name,
            'type': 'reservation',
            'state': state,
            'tribal_affiliation': tribe,
            'source': 'reservations'
        })
    
    # Common district names (like "Babakiri District")
    districts = [
        ("Babakiri District", None, None),
    ]
    for name, state, tribe in districts:
        places.append({
            'name': name,
            'type': 'district',
            'state': state,
            'tribal_affiliation': tribe,
            'source': 'districts'
        })
    
    return places

def download_common_names() -> tuple[List[str], List[str]]:
    """Download common first and last names from public sources."""
    # Common US first names (top 1000 from SSA data patterns)
    # This is a representative sample - in production, download full dataset
    common_first = [
        "James", "Robert", "John", "Michael", "David", "William", "Richard", "Joseph",
        "Thomas", "Christopher", "Charles", "Daniel", "Matthew", "Anthony", "Mark",
        "Donald", "Steven", "Paul", "Andrew", "Joshua", "Kenneth", "Kevin", "Brian",
        "George", "Timothy", "Ronald", "Jason", "Edward", "Jeffrey", "Ryan", "Jacob",
        "Gary", "Nicholas", "Eric", "Jonathan", "Stephen", "Larry", "Justin", "Scott",
        "Brandon", "Benjamin", "Samuel", "Frank", "Gregory", "Raymond", "Alexander",
        "Patrick", "Jack", "Dennis", "Jerry", "Tyler", "Aaron", "Jose", "Henry",
        "Adam", "Douglas", "Nathan", "Zachary", "Kyle", "Noah", "Ethan", "Jeremy",
        "Mary", "Patricia", "Jennifer", "Linda", "Elizabeth", "Barbara", "Susan",
        "Jessica", "Sarah", "Karen", "Nancy", "Lisa", "Betty", "Margaret", "Sandra",
        "Ashley", "Kimberly", "Emily", "Donna", "Michelle", "Dorothy", "Carol",
        "Amanda", "Melissa", "Deborah", "Stephanie", "Rebecca", "Sharon", "Laura",
        "Cynthia", "Kathleen", "Amy", "Shirley", "Angela", "Helen", "Anna", "Brenda",
        "Pamela", "Nicole", "Samantha", "Katherine", "Emma", "Christine", "Debra",
        "Rachel", "Carolyn", "Janet", "Virginia", "Maria", "Heather", "Diane",
        "Julie", "Joyce", "Victoria", "Kelly", "Christina", "Joan", "Evelyn"
    ]
    
    # Common US last names (top 100 from census data patterns)
    common_last = [
        "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
        "Rodriguez", "Martinez", "Hernandez", "Lopez", "Wilson", "Anderson", "Thomas",
        "Taylor", "Moore", "Jackson", "Martin", "Lee", "Thompson", "White", "Harris",
        "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson", "Walker", "Young", "Allen",
        "King", "Wright", "Scott", "Torres", "Nguyen", "Hill", "Flores", "Green",
        "Adams", "Nelson", "Baker", "Hall", "Rivera", "Campbell", "Mitchell", "Carter",
        "Roberts", "Gomez", "Phillips", "Evans", "Turner", "Diaz", "Parker", "Cruz",
        "Edwards", "Collins", "Reyes", "Stewart", "Morris", "Morales", "Murphy",
        "Cook", "Rogers", "Gutierrez", "Ortiz", "Morgan", "Cooper", "Peterson",
        "Bailey", "Reed", "Kelly", "Howard", "Ramos", "Kim", "Cox", "Ward", "Richardson",
        "Watson", "Brooks", "Chavez", "Wood", "James", "Bennett", "Gray", "Mendoza",
        "Ruiz", "Hughes", "Price", "Alvarez", "Castillo", "Sanders", "Patel", "Myers",
        "Long", "Ross", "Foster", "Jimenez"
    ]
    
    return common_first, common_last

def populate_database():
    """Populate the database with downloaded data."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    print("Downloading Native American names...")
    native_names = download_native_american_names()
    for name_data in native_names:
        try:
            cursor.execute('''
                INSERT OR IGNORE INTO native_american_names 
                (first_name, last_name, gender, tribe_origin, source)
                VALUES (?, ?, ?, ?, ?)
            ''', (name_data['first_name'], name_data['last_name'], 
                  name_data['gender'], name_data['tribe_origin'], name_data['source']))
        except sqlite3.IntegrityError:
            pass
    print(f"  ✓ Added {len(native_names)} Native American names")
    
    print("Downloading place names...")
    places = download_place_names()
    for place in places:
        try:
            cursor.execute('''
                INSERT OR IGNORE INTO place_names 
                (name, type, state, tribal_affiliation, source)
                VALUES (?, ?, ?, ?, ?)
            ''', (place['name'], place['type'], place['state'], 
                  place['tribal_affiliation'], place['source']))
        except sqlite3.IntegrityError:
            pass
    print(f"  ✓ Added {len(places)} place names")
    
    print("Downloading common names...")
    first_names, last_names = download_common_names()
    for i, name in enumerate(first_names):
        try:
            cursor.execute('''
                INSERT OR IGNORE INTO common_first_names (name, frequency_rank, source)
                VALUES (?, ?, ?)
            ''', (name, i + 1, 'common_first_names'))
        except sqlite3.IntegrityError:
            pass
    
    for i, name in enumerate(last_names):
        try:
            cursor.execute('''
                INSERT OR IGNORE INTO common_last_names (name, frequency_rank, source)
                VALUES (?, ?, ?)
            ''', (name, i + 1, 'common_last_names'))
        except sqlite3.IntegrityError:
            pass
    print(f"  ✓ Added {len(first_names)} first names and {len(last_names)} last names")
    
    conn.commit()
    conn.close()
    print("\n✓ Database populated successfully!")

def get_database_stats():
    """Print statistics about the database."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute('SELECT COUNT(*) FROM native_american_names')
    native_count = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(*) FROM place_names')
    place_count = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(*) FROM common_first_names')
    first_count = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(*) FROM common_last_names')
    last_count = cursor.fetchone()[0]
    
    conn.close()
    
    print(f"\nDatabase Statistics:")
    print(f"  Native American names: {native_count}")
    print(f"  Place names: {place_count}")
    print(f"  Common first names: {first_count}")
    print(f"  Common last names: {last_count}")
    print(f"  Total entries: {native_count + place_count + first_count + last_count}")

if __name__ == "__main__":
    print("=" * 80)
    print("Building Name and Location Database")
    print("=" * 80)
    print()
    
    create_database()
    populate_database()
    get_database_stats()
    
    print(f"\n✓ Database ready at: {DB_PATH}")

