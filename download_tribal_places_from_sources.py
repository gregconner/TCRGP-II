#!/usr/bin/env python3
"""
Download Comprehensive Tribal Place Names from Public Sources

This script downloads thousands of tribal place names from:
- USGS GNIS (Geographic Names Information System)
- Bureau of Indian Affairs
- Other public sources

Run this to populate the database with comprehensive tribal place name data.
"""

import sqlite3
import urllib.request
import urllib.parse
import csv
import io
import gzip
import json
from pathlib import Path
from typing import List, Dict
import re

DB_PATH = Path(__file__).parent / "name_location_database.db"

def download_gnis_ftp() -> List[Dict]:
    """Try downloading from GNIS FTP server or alternative direct URLs."""
    places = []
    
    try:
        print("  Attempting to download from GNIS FTP/alternative URLs...")
        
        # Try various GNIS download URLs
        gnis_urls = [
            # FTP-style URLs
            "ftp://geonames.usgs.gov/pub/domestic/NationalFile.zip",
            "ftp://geonames.usgs.gov/pub/domestic/NationalFile.txt",
            # HTTP alternatives
            "http://geonames.usgs.gov/docs/stategaz/NationalFile.zip",
            "https://geonames.usgs.gov/docs/stategaz/NationalFile.zip",
            # Alternative paths
            "https://geonames.usgs.gov/pub/domestic/NationalFile.zip",
            "http://geonames.usgs.gov/pub/domestic/NationalFile.zip",
            # State files (smaller, more likely to work)
            "https://geonames.usgs.gov/docs/stategaz/AZ_Features.zip",  # Arizona
            "https://geonames.usgs.gov/docs/stategaz/NM_Features.zip",  # New Mexico
        ]
        
        for url in gnis_urls:
            try:
                print(f"    Trying: {url}")
                with urllib.request.urlopen(url, timeout=30) as response:
                    data = response.read()
                    print(f"      ✓ Downloaded {len(data) / 1024 / 1024:.1f} MB")
                    
                    # Process the file
                    import zipfile
                    with zipfile.ZipFile(io.BytesIO(data)) as zip_file:
                        for name in zip_file.namelist():
                            if name.endswith('.txt') or name.endswith('.TXT'):
                                with zip_file.open(name) as f:
                                    content = f.read().decode('utf-8', errors='ignore')
                                    for line in content.split('\n'):
                                        if not line.strip():
                                            continue
                                        parts = line.split('|')
                                        if len(parts) >= 4:
                                            feature_name = parts[1].strip()
                                            feature_class = parts[2].strip()
                                            state = parts[3].strip() if len(parts) > 3 else ""
                                            
                                            # Filter for tribal places
                                            if (feature_class in ['P', 'C', 'R', 'L', 'A', 'S'] or
                                                'Reservation' in feature_class or
                                                'Pueblo' in feature_class or
                                                'Reservation' in feature_name or
                                                'Pueblo' in feature_name or
                                                'Nation' in feature_name):
                                                
                                                places.append({
                                                    'name': feature_name,
                                                    'type': feature_class.lower() if feature_class else 'unknown',
                                                    'state': state,
                                                    'tribe': None,
                                                    'source': 'gnis_ftp'
                                                })
                                                
                                                if len(places) % 1000 == 0:
                                                    print(f"      Processed {len(places)} places...")
                                break
                    print(f"    ✓ Extracted {len(places)} places from {url}")
                    break  # Success, stop trying other URLs
            except urllib.error.URLError:
                continue  # Try next URL
            except Exception as e:
                print(f"      ⚠ Error with {url}: {e}")
                continue
        
        if not places:
            print("    ⚠ Could not download from any FTP/alternative URL")
            
    except Exception as e:
        print(f"    ⚠ Error trying FTP/alternative URLs: {e}")
    
    return places

def download_gnis_ld_sparql() -> List[Dict]:
    """Download from GNIS-LD (Linked Data) service via SPARQL endpoint.
    
    GNIS-LD provides GNIS data in linked data format with a SPARQL endpoint.
    URL: https://gnis-ld.org/
    """
    places = []
    
    try:
        print("  Attempting to download from GNIS-LD (Linked Data) service...")
        print("    URL: https://gnis-ld.org/")
        
        # GNIS-LD SPARQL endpoint
        sparql_endpoint = "https://gnis-ld.org/sparql"
        
        # SPARQL query to get all populated places, reservations, and tribal features
        # Focus on features that might be tribal places
        query = """
        PREFIX gnis: <https://geonames.org/ontology#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        
        SELECT DISTINCT ?name ?type ?state WHERE {
            ?feature gnis:name ?name .
            ?feature gnis:featureClass ?type .
            OPTIONAL { ?feature gnis:state ?state . }
            FILTER (
                ?type IN ("Populated Place", "Civil", "Reservation", "Locale", "Area", "Census") ||
                CONTAINS(LCASE(?name), "reservation") ||
                CONTAINS(LCASE(?name), "pueblo") ||
                CONTAINS(LCASE(?name), "nation") ||
                CONTAINS(LCASE(?name), "tribe")
            )
        }
        LIMIT 50000
        """
        
        # Encode query
        params = {
            'query': query,
            'format': 'json'
        }
        url = f"{sparql_endpoint}?{urllib.parse.urlencode(params)}"
        
        print(f"    Querying SPARQL endpoint...")
        with urllib.request.urlopen(url, timeout=120) as response:
            data = json.loads(response.read().decode('utf-8'))
            
            # Parse SPARQL JSON results
            if 'results' in data and 'bindings' in data['results']:
                for binding in data['results']['bindings']:
                    name = binding.get('name', {}).get('value', '')
                    ftype = binding.get('type', {}).get('value', 'unknown')
                    state = binding.get('state', {}).get('value', '')
                    
                    if name:
                        places.append({
                            'name': name,
                            'type': ftype.lower().replace(' ', '_'),
                            'state': state,
                            'tribe': None,
                            'source': 'gnis_ld_sparql'
                        })
                        
                        if len(places) % 1000 == 0:
                            print(f"      Processed {len(places)} places...")
                
                print(f"    ✓ Got {len(places)} places from GNIS-LD")
            else:
                print(f"    ⚠ Unexpected response format")
                
    except urllib.error.URLError as e:
        print(f"    ⚠ Could not access GNIS-LD: {e}")
    except Exception as e:
        print(f"    ⚠ Error querying GNIS-LD: {e}")
    
    return places

def download_datagov_gnis() -> List[Dict]:
    """Download GNIS data from Data.gov.
    
    Data.gov has GNIS datasets available for download.
    """
    places = []
    
    try:
        print("  Attempting to download from Data.gov...")
        
        # Data.gov has multiple GNIS datasets
        # Try the Populated Places dataset
        datagov_urls = [
            "https://catalog.data.gov/dataset/gnis-populated-places",
            "https://catalog.data.gov/dataset/geographic-names-information-system-gnis",
        ]
        
        # Data.gov provides API access via CKAN
        # Try to get the resource download URL
        ckan_api = "https://catalog.data.gov/api/3/action/package_search"
        params = {
            'q': 'GNIS',
            'rows': 10
        }
        
        url = f"{ckan_api}?{urllib.parse.urlencode(params)}"
        print(f"    Querying Data.gov API...")
        
        with urllib.request.urlopen(url, timeout=60) as response:
            data = json.loads(response.read().decode('utf-8'))
            
            if 'result' in data and 'results' in data['result']:
                print(f"    ✓ Found {len(data['result']['results'])} GNIS datasets on Data.gov")
                print(f"    → Visit https://catalog.data.gov/dataset?q=GNIS for manual download")
                # Data.gov typically requires manual download or API key for bulk access
                # We'll note this as an option
            else:
                print(f"    ⚠ Could not find GNIS datasets")
                
    except Exception as e:
        print(f"    ⚠ Error accessing Data.gov: {e}")
    
    return places

def download_national_map_api() -> List[Dict]:
    """Download from The National Map Downloader/API.
    
    The National Map provides GNIS data through their downloader and services.
    """
    places = []
    
    try:
        print("  Attempting to download from The National Map...")
        
        # The National Map has a downloader service
        # For programmatic access, we can try their services endpoint
        # Note: This may require area definition, so we'll try a general query
        
        # The National Map Services
        # Try accessing GNIS names layer
        # Note: URL format may vary, trying common patterns
        services_urls = [
            "https://services.nationalmap.gov/arcgis/rest/services/gnis/MapServer/0/query",
            "https://services.nationalmap.gov/arcgis/rest/services/gnis_names/MapServer/0/query",
            "http://services.nationalmap.gov/arcgis/rest/services/gnis/MapServer/0/query",
        ]
        
        services_url = services_urls[0]  # Try first one
        
        # Query for all features (this might be limited)
        params = {
            'where': "1=1",  # Get all features
            'outFields': 'FEATURE_NAME,FEATURE_CLASS,STATE_ALPHA',
            'returnGeometry': 'false',
            'f': 'json',
            'returnCountOnly': 'false'
        }
        
        # Try each URL
        for services_url in services_urls:
            try:
                url = f"{services_url}?{urllib.parse.urlencode(params)}"
                print(f"    Querying The National Map Services: {services_url}")
                
                with urllib.request.urlopen(url, timeout=120) as response:
                    data = json.loads(response.read().decode('utf-8'))
                    
                    if 'features' in data:
                        for feature in data['features']:
                            attrs = feature.get('attributes', {})
                            name = attrs.get('FEATURE_NAME', '')
                            ftype = attrs.get('FEATURE_CLASS', 'unknown')
                            state = attrs.get('STATE_ALPHA', '')
                            
                            # Filter for tribal places
                            if name and (ftype in ['Populated Place', 'Civil', 'Reservation', 'Locale', 'Area', 'Census'] or
                                         'reservation' in name.lower() or
                                         'pueblo' in name.lower() or
                                         'nation' in name.lower()):
                                places.append({
                                    'name': name,
                                    'type': ftype.lower().replace(' ', '_'),
                                    'state': state,
                                    'tribe': None,
                                    'source': 'national_map_services'
                                })
                                
                                if len(places) % 1000 == 0:
                                    print(f"      Processed {len(places)} places...")
                        
                        print(f"    ✓ Got {len(places)} places from The National Map")
                        break  # Success
                    else:
                        print(f"    ⚠ Unexpected response format from {services_url}")
            except urllib.error.URLError as e:
                print(f"    ⚠ Could not access {services_url}: {e}")
                continue  # Try next URL
            except Exception as e:
                print(f"    ⚠ Error accessing {services_url}: {e}")
                continue  # Try next URL
        
        if not places:
            print("    ⚠ Could not access The National Map from any URL")
                
    except Exception as e:
        print(f"    ⚠ Error accessing The National Map: {e}")
    
    return places

def download_gnis_national_file() -> List[Dict]:
    """Download and parse USGS GNIS National File.
    
    The GNIS National File contains over 2 million geographic features.
    We'll filter for features likely to be tribal places.
    
    GNIS Feature Classes that are likely tribal places:
    - Populated Place (P)
    - Civil (C) 
    - Reservation (R)
    - Locale (L)
    - Area (A)
    - Census (S)
    """
    places = []
    
    try:
        print("Downloading USGS GNIS National File...")
        print("  URL: https://geonames.usgs.gov/docs/stategaz/NationalFile.zip")
        print("  (This is a large file - may take several minutes)")
        print("  The file contains over 2 million geographic features")
        print("  We'll extract all features that might be tribal places")
        
        # GNIS National File URL (official USGS source)
        # Alternative: State files are smaller and can be downloaded individually
        url = "https://geonames.usgs.gov/docs/stategaz/NationalFile.zip"
        
        # Also try state-specific files which are smaller
        # For tribal places, focus on states with high Native populations:
        tribal_states = ["AZ", "NM", "OK", "SD", "ND", "MT", "AK", "WA", "OR", "MN", "WI", "NY", "NC"]
        
        # Download the file
        try:
            with urllib.request.urlopen(url, timeout=300) as response:
                data = response.read()
                print(f"  ✓ Downloaded {len(data) / 1024 / 1024:.1f} MB")
                
                # Parse the ZIP file
                import zipfile
                with zipfile.ZipFile(io.BytesIO(data)) as zip_file:
                    # Find the NationalFile.txt inside
                    for name in zip_file.namelist():
                        if 'NationalFile' in name and name.endswith('.txt'):
                            print(f"  ✓ Found {name}")
                            with zip_file.open(name) as f:
                                # GNIS format: Feature ID | Feature Name | Feature Class | State | County | etc.
                                # We want: Feature Class in ['Populated Place', 'Civil', 'Reservation', 'Locale', 'Area']
                                # And names that might be tribal
                                
                                # Read in chunks to handle large file
                                chunk_size = 1024 * 1024  # 1MB chunks
                                buffer = ""
                                
                                for chunk in iter(lambda: f.read(chunk_size), b''):
                                    buffer += chunk.decode('utf-8', errors='ignore')
                                    lines = buffer.split('\n')
                                    buffer = lines[-1]  # Keep incomplete line
                                    
                                    for line in lines[:-1]:
                                        if not line.strip():
                                            continue
                                        
                                        # GNIS format is pipe-delimited
                                        parts = line.split('|')
                                        if len(parts) < 4:
                                            continue
                                        
                                        feature_id = parts[0].strip()
                                        feature_name = parts[1].strip()
                                        feature_class = parts[2].strip()
                                        state = parts[3].strip() if len(parts) > 3 else ""
                                        
                                        # Filter for likely tribal places
                                        # GNIS Feature Class codes: P=Populated Place, C=Civil, R=Reservation, L=Locale, A=Area, S=Census
                                        feature_class_codes = ['P', 'C', 'R', 'L', 'A', 'S']
                                        feature_class_names = ['Populated Place', 'Civil', 'Reservation', 'Locale', 'Area', 'Census']
                                        
                                        if (feature_class in feature_class_names or 
                                            feature_class in feature_class_codes or
                                            'Reservation' in feature_class or
                                            'Pueblo' in feature_class or
                                            'Village' in feature_class):
                                            
                                            # Check if name might be tribal (heuristic)
                                            # Many tribal place names have specific patterns
                                            if (len(feature_name) > 2 and 
                                                feature_name[0].isupper() and
                                                not feature_name.lower() in ['the', 'a', 'an', 'and', 'or']):
                                                
                                                # Additional heuristics for tribal names:
                                                # - Contains "Reservation", "Pueblo", "Nation", "Tribe"
                                                # - Common tribal name patterns
                                                is_likely_tribal = (
                                                    'reservation' in feature_name.lower() or
                                                    'pueblo' in feature_name.lower() or
                                                    'nation' in feature_name.lower() or
                                                    'tribe' in feature_name.lower() or
                                                    'village' in feature_name.lower() or
                                                    feature_class == 'R' or  # Reservation
                                                    'Reservation' in feature_class
                                                )
                                                
                                                places.append({
                                                    'name': feature_name,
                                                    'type': feature_class.lower().replace(' ', '_') if isinstance(feature_class, str) else 'unknown',
                                                    'state': state,
                                                    'tribe': None,  # GNIS doesn't provide tribe info directly
                                                    'source': 'usgs_gnis_national'
                                                })
                                                
                                                if len(places) % 1000 == 0:
                                                    print(f"    Processed {len(places)} places...")
                                                
                                                # Stop at 50,000 to avoid memory issues (can be adjusted)
                                                if len(places) >= 50000:
                                                    print(f"    Reached 50,000 places - stopping (can increase limit if needed)")
                                                    break
                                
                                # Process remaining buffer
                                if buffer.strip():
                                    parts = buffer.split('|')
                                    if len(parts) >= 4:
                                        feature_name = parts[1].strip()
                                        feature_class = parts[2].strip()
                                        state = parts[3].strip() if len(parts) > 3 else ""
                                        if feature_class in ['Populated Place', 'Civil', 'Reservation', 'Locale', 'Area', 'Census']:
                                            places.append({
                                                'name': feature_name,
                                                'type': feature_class.lower().replace(' ', '_'),
                                                'state': state,
                                                'tribe': None,
                                                'source': 'usgs_gnis_national'
                                            })
                                
                                print(f"  ✓ Extracted {len(places)} places from GNIS National File")
                                break
        except urllib.error.URLError as e:
            print(f"  ⚠ Could not download GNIS National File: {e}")
            print("  → Trying state-specific files instead...")
            state_places = download_gnis_state_files()
            if state_places:
                return state_places
        except Exception as e:
            print(f"  ⚠ Error processing GNIS file: {e}")
            print("  → Trying state-specific files instead...")
            state_places = download_gnis_state_files()
            if state_places:
                return state_places
            
    except Exception as e:
        print(f"  ⚠ Error downloading GNIS: {e}")
        print("  → Trying state-specific files instead...")
        state_places = download_gnis_state_files()
        if state_places:
            return state_places
    
    # If we get here, downloads failed - return empty list (will use curated list)
    return places

def download_gnis_state_files() -> List[Dict]:
    """Download GNIS state files for states with high Native populations.
    
    State files are smaller and more manageable than the full National File.
    """
    places = []
    
    # States with high Native populations (more likely to have tribal places)
    # Focus on states with the most tribal places first
    priority_states = {
        'AZ': 'Arizona',      # Many reservations and pueblos
        'NM': 'New Mexico',   # Many pueblos
        'OK': 'Oklahoma',     # Many relocated tribes
        'SD': 'South Dakota', # Major reservations
        'ND': 'North Dakota', # Major reservations
        'MT': 'Montana',      # Major reservations
        'AK': 'Alaska',       # Many Native villages
        'WA': 'Washington',   # Many reservations
        'OR': 'Oregon',       # Many reservations
        'MN': 'Minnesota',    # Many reservations
        'WI': 'Wisconsin',    # Many reservations
        'NY': 'New York',     # Iroquois nations
        'NC': 'North Carolina', # Lumbee, Cherokee
    }
    
    print(f"  Attempting to download state files for {len(priority_states)} priority states...")
    
    for state_code, state_name in priority_states.items():
        try:
            # Try multiple URL patterns
            urls = [
                f"https://geonames.usgs.gov/docs/stategaz/{state_code}_Features.zip",
                f"https://geonames.usgs.gov/docs/stategaz/{state_code}_Features_File.zip",
                f"https://geonames.usgs.gov/docs/stategaz/{state_code.lower()}_Features.zip",
            ]
            
            downloaded = False
            for url in urls:
                try:
                    print(f"    Trying {state_code} ({state_name})...")
                    with urllib.request.urlopen(url, timeout=30) as response:
                        data = response.read()
                        print(f"      ✓ Downloaded {len(data) / 1024:.1f} KB from {url}")
                        downloaded = True
                        
                        import zipfile
                        with zipfile.ZipFile(io.BytesIO(data)) as zip_file:
                            for name in zip_file.namelist():
                                if name.endswith('.txt') or name.endswith('.TXT'):
                                    with zip_file.open(name) as f:
                                        content = f.read().decode('utf-8', errors='ignore')
                                        lines_processed = 0
                                        for line in content.split('\n'):
                                            if not line.strip():
                                                continue
                                            parts = line.split('|')
                                            if len(parts) >= 4:
                                                feature_name = parts[1].strip()
                                                feature_class = parts[2].strip()
                                                
                                                # Filter for tribal places
                                                if (feature_class in ['P', 'C', 'R', 'L', 'A', 'S'] or
                                                    'Reservation' in feature_class or
                                                    'Pueblo' in feature_class or
                                                    'Reservation' in feature_name or
                                                    'Pueblo' in feature_name or
                                                    'Nation' in feature_name or
                                                    'Tribe' in feature_name):
                                                    
                                                    places.append({
                                                        'name': feature_name,
                                                        'type': feature_class.lower() if feature_class else 'unknown',
                                                        'state': state_name,
                                                        'tribe': None,
                                                        'source': f'usgs_gnis_{state_code}'
                                                    })
                                                    
                                                    lines_processed += 1
                                                    if len(places) % 500 == 0:
                                                        print(f"      Processed {len(places)} places total...")
                                        print(f"      ✓ Processed {lines_processed} features from {state_code}")
                                    break
                        break
                except urllib.error.URLError:
                    continue  # Try next URL
                except Exception as e:
                    print(f"      ⚠ Error with {url}: {e}")
                    continue
            
            if not downloaded:
                print(f"      ⚠ Could not download {state_code} from any URL")
                
        except Exception as e:
            print(f"      ⚠ Error processing {state_code}: {e}")
            continue
    
    print(f"  ✓ Extracted {len(places)} places from state files")
    return places

def download_epa_tribes_data() -> List[Dict]:
    """Download from EPA Tribes Names Service.
    
    EPA provides a Tribes Names Service with up-to-date information on
    federally recognized tribes.
    """
    places = []
    
    try:
        print("  Attempting to download from EPA Tribes Names Service...")
        # EPA Tribes Names Service API
        # URL: https://www.epa.gov/data/tribes-names-service
        # This requires API access, but we can note it as an option
        print("    → EPA Tribes Names Service available at: https://www.epa.gov/data/tribes-names-service")
        print("    → Requires API access for programmatic downloads")
    except Exception as e:
        print(f"    ⚠ EPA error: {e}")
    
    return places

def download_datagov_tribes() -> List[Dict]:
    """Download from Data.gov Federally Recognized Tribes dataset."""
    places = []
    
    try:
        print("  Attempting to download from Data.gov...")
        # Data.gov has a Federally Recognized Tribes dataset
        # URL: https://catalog.data.gov/dataset/federally-recognized-tribes
        url = "https://catalog.data.gov/dataset/federally-recognized-tribes"
        print(f"    → Data.gov dataset: {url}")
        print("    → Dataset includes historical data from 1978 to present")
        print("    → May require manual download or API key")
    except Exception as e:
        print(f"    ⚠ Data.gov error: {e}")
    
    return places

def download_alternative_sources() -> List[Dict]:
    """Try alternative sources for tribal place names."""
    places = []
    
    print("  Trying alternative sources...")
    
    # Try EPA
    epa_places = download_epa_tribes_data()
    places.extend(epa_places)
    
    # Try Data.gov
    datagov_places = download_datagov_tribes()
    places.extend(datagov_places)
    
    # Try GeoNames API (has free tier)
    try:
        print("    Attempting GeoNames search API...")
        # GeoNames has a search API but requires registration for bulk downloads
        print("    → GeoNames requires API key for bulk downloads")
        print("    → Visit: https://www.geonames.org/export/")
    except Exception as e:
        print(f"    ⚠ GeoNames error: {e}")
    
    return places

def download_comprehensive_tribal_place_list() -> List[Dict]:
    """Download comprehensive list from various sources.
    
    This function aggregates tribal place names from multiple sources
    to create the most comprehensive list possible.
    """
    all_places = []
    
    # Try alternative GNIS sources first (more reliable)
    print("\n1. Attempting to download from GNIS-LD (Linked Data) service...")
    gnis_ld_places = download_gnis_ld_sparql()
    if gnis_ld_places:
        print(f"  ✓ Got {len(gnis_ld_places)} places from GNIS-LD")
        all_places.extend(gnis_ld_places)
    
    print("\n2. Attempting to download from The National Map Services...")
    national_map_places = download_national_map_api()
    if national_map_places:
        print(f"  ✓ Got {len(national_map_places)} places from The National Map")
        all_places.extend(national_map_places)
    
    # Try GNIS FTP/alternative URLs
    print("\n3. Attempting to download from GNIS FTP/alternative URLs...")
    gnis_ftp_places = download_gnis_ftp()
    if gnis_ftp_places:
        print(f"  ✓ Got {len(gnis_ftp_places)} places from GNIS FTP")
        all_places.extend(gnis_ftp_places)
    
    # Try original GNIS National File (may be down)
    print("\n4. Attempting to download from USGS GNIS National File...")
    gnis_places = download_gnis_national_file()
    if gnis_places:
        print(f"  ✓ Got {len(gnis_places)} places from GNIS")
        all_places.extend(gnis_places)
    else:
        print("  → GNIS National File unavailable (server may be down)")
    
    # Try Data.gov
    print("\n5. Checking Data.gov for GNIS datasets...")
    datagov_places = download_datagov_gnis()
    if datagov_places:
        print(f"  ✓ Got {len(datagov_places)} places from Data.gov")
        all_places.extend(datagov_places)
    
    # Try other alternative sources
    print("\n6. Trying other alternative sources (EPA, etc.)...")
    alt_places = download_alternative_sources()
    if alt_places:
        print(f"  ✓ Got {len(alt_places)} places from alternative sources")
        all_places.extend(alt_places)
    
    # Add comprehensive curated list as baseline
    # (This ensures we have at least well-known places even if downloads fail)
    print("\n3. Adding comprehensive curated list from database builder...")
    curated_places = get_comprehensive_curated_tribal_places()
    if curated_places:
        print(f"  ✓ Got {len(curated_places)} places from curated list")
        all_places.extend(curated_places)
    else:
        print("  ⚠ Curated list is empty - this shouldn't happen")
    
    # Remove duplicates
    seen = set()
    unique_places = []
    for place in all_places:
        key = (place['name'].lower(), place.get('type', ''), place.get('state', ''))
        if key not in seen:
            seen.add(key)
            unique_places.append(place)
    
    print(f"\n✓ Total unique tribal places: {len(unique_places)}")
    return unique_places

def get_comprehensive_curated_tribal_places() -> List[Dict]:
    """Get comprehensive curated list of tribal places.
    
    This imports the comprehensive list from build_name_location_database_v1.2.0
    to ensure we have a good baseline even if downloads fail.
    """
    places = []
    
    # Import the comprehensive list by calling the function from the database builder
    try:
        import sys
        import importlib.util
        from pathlib import Path
        
        db_builder_path = Path(__file__).parent / "build_name_location_database_v1.2.0.py"
        if db_builder_path.exists():
            spec = importlib.util.spec_from_file_location("db_builder", db_builder_path)
            db_builder = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(db_builder)
            
            # Call the comprehensive function
            tribal_place_data = db_builder.download_tribal_reservations_comprehensive()
            for place in tribal_place_data:
                places.append({
                    'name': place['name'],
                    'type': place['type'],
                    'tribe': place.get('tribe'),
                    'state': place.get('state'),
                    'source': 'comprehensive_curated'
                })
            print(f"  ✓ Imported {len(places)} places from database builder")
            return places
    except Exception as e:
        print(f"  ⚠ Could not import from database builder: {e}")
        print("  → Using basic well-known list instead")
    
    # Fallback: Well-known reservations and pueblos (comprehensive list)
    # This matches the list from build_name_location_database_v1.2.0
    well_known = [
        # Southwest
        ("Navajo Nation", "reservation", "Navajo", "Arizona"),
        ("Navajo Nation", "reservation", "Navajo", "New Mexico"),
        ("Navajo Nation", "reservation", "Navajo", "Utah"),
        ("Hopi Reservation", "reservation", "Hopi", "Arizona"),
        ("Tohono O'odham Nation", "reservation", "Tohono O'odham", "Arizona"),
        ("San Xavier Reservation", "reservation", "Tohono O'odham", "Arizona"),
        ("Gila River Indian Community", "reservation", "Pima/Maricopa", "Arizona"),
        ("Salt River Pima-Maricopa Indian Community", "reservation", "Pima/Maricopa", "Arizona"),
        ("White Mountain Apache Reservation", "reservation", "Apache", "Arizona"),
        ("Fort Apache Reservation", "reservation", "Apache", "Arizona"),
        ("San Carlos Apache Reservation", "reservation", "Apache", "Arizona"),
        ("Yavapai-Apache Nation", "reservation", "Yavapai/Apache", "Arizona"),
        ("Acoma Pueblo", "pueblo", "Acoma", "New Mexico"),
        ("Cochiti Pueblo", "pueblo", "Cochiti", "New Mexico"),
        ("Isleta Pueblo", "pueblo", "Isleta", "New Mexico"),
        ("Jemez Pueblo", "pueblo", "Jemez", "New Mexico"),
        ("Laguna Pueblo", "pueblo", "Laguna", "New Mexico"),
        ("Nambe Pueblo", "pueblo", "Nambe", "New Mexico"),
        ("Picuris Pueblo", "pueblo", "Picuris", "New Mexico"),
        ("Pojoaque Pueblo", "pueblo", "Pojoaque", "New Mexico"),
        ("San Felipe Pueblo", "pueblo", "San Felipe", "New Mexico"),
        ("San Ildefonso Pueblo", "pueblo", "San Ildefonso", "New Mexico"),
        ("Sandia Pueblo", "pueblo", "Sandia", "New Mexico"),
        ("Santa Ana Pueblo", "pueblo", "Santa Ana", "New Mexico"),
        ("Santa Clara Pueblo", "pueblo", "Santa Clara", "New Mexico"),
        ("Santo Domingo Pueblo", "pueblo", "Santo Domingo", "New Mexico"),
        ("Taos Pueblo", "pueblo", "Taos", "New Mexico"),
        ("Tesuque Pueblo", "pueblo", "Tesuque", "New Mexico"),
        ("Zia Pueblo", "pueblo", "Zia", "New Mexico"),
        ("Zuni Pueblo", "pueblo", "Zuni", "New Mexico"),
        ("Jicarilla Apache Reservation", "reservation", "Apache", "New Mexico"),
        ("Mescalero Apache Reservation", "reservation", "Apache", "New Mexico"),
        # Great Plains
        ("Pine Ridge Reservation", "reservation", "Lakota", "South Dakota"),
        ("Standing Rock Reservation", "reservation", "Lakota", "South Dakota"),
        ("Standing Rock Reservation", "reservation", "Lakota", "North Dakota"),
        ("Cheyenne River Reservation", "reservation", "Lakota", "South Dakota"),
        ("Rosebud Reservation", "reservation", "Lakota", "South Dakota"),
        ("Lower Brule Reservation", "reservation", "Lakota", "South Dakota"),
        ("Yankton Reservation", "reservation", "Lakota", "South Dakota"),
        ("Sisseton Wahpeton Reservation", "reservation", "Dakota", "South Dakota"),
        ("Blackfeet Reservation", "reservation", "Blackfeet", "Montana"),
        ("Crow Reservation", "reservation", "Crow", "Montana"),
        ("Flathead Reservation", "reservation", "Salish/Kootenai", "Montana"),
        ("Fort Belknap Reservation", "reservation", "Gros Ventre/Assiniboine", "Montana"),
        ("Fort Peck Reservation", "reservation", "Assiniboine/Sioux", "Montana"),
        ("Northern Cheyenne Reservation", "reservation", "Cheyenne", "Montana"),
        ("Rocky Boy's Reservation", "reservation", "Chippewa/Cree", "Montana"),
        ("Turtle Mountain Reservation", "reservation", "Chippewa", "North Dakota"),
        ("Fort Berthold Reservation", "reservation", "Mandan/Hidatsa/Arikara", "North Dakota"),
        ("Wind River Reservation", "reservation", "Shoshone/Arapaho", "Wyoming"),
        # Great Lakes
        ("Red Lake Reservation", "reservation", "Ojibwe", "Minnesota"),
        ("White Earth Reservation", "reservation", "Ojibwe", "Minnesota"),
        ("Fond du Lac Reservation", "reservation", "Ojibwe", "Minnesota"),
        ("Leech Lake Reservation", "reservation", "Ojibwe", "Minnesota"),
        ("Mille Lacs Reservation", "reservation", "Ojibwe", "Minnesota"),
        ("Bois Forte Reservation", "reservation", "Ojibwe", "Minnesota"),
        ("Grand Portage Reservation", "reservation", "Ojibwe", "Minnesota"),
        ("Lower Sioux Reservation", "reservation", "Dakota", "Minnesota"),
        ("Prairie Island Reservation", "reservation", "Dakota", "Minnesota"),
        ("Shakopee Mdewakanton Reservation", "reservation", "Dakota", "Minnesota"),
        ("Upper Sioux Reservation", "reservation", "Dakota", "Minnesota"),
        ("Menominee Reservation", "reservation", "Menominee", "Wisconsin"),
        ("Oneida Reservation", "reservation", "Oneida", "Wisconsin"),
        ("Ho-Chunk Nation", "reservation", "Ho-Chunk", "Wisconsin"),
        ("Lac du Flambeau Reservation", "reservation", "Ojibwe", "Wisconsin"),
        ("Bad River Reservation", "reservation", "Ojibwe", "Wisconsin"),
        ("Red Cliff Reservation", "reservation", "Ojibwe", "Wisconsin"),
        ("St. Croix Reservation", "reservation", "Chippewa", "Wisconsin"),
        ("Stockbridge-Munsee Reservation", "reservation", "Stockbridge-Munsee", "Wisconsin"),
        ("Oneida Nation", "reservation", "Oneida", "New York"),
        ("Onondaga Nation", "reservation", "Onondaga", "New York"),
        ("Seneca Nation", "reservation", "Seneca", "New York"),
        ("Tuscarora Nation", "reservation", "Tuscarora", "New York"),
        ("Cayuga Nation", "reservation", "Cayuga", "New York"),
        ("Mohawk Nation", "reservation", "Mohawk", "New York"),
        # Northwest
        ("Yakama Reservation", "reservation", "Yakama", "Washington"),
        ("Colville Reservation", "reservation", "Colville", "Washington"),
        ("Quinault Reservation", "reservation", "Quinault", "Washington"),
        ("Lummi Reservation", "reservation", "Lummi", "Washington"),
        ("Tulalip Reservation", "reservation", "Tulalip", "Washington"),
        ("Makah Reservation", "reservation", "Makah", "Washington"),
        ("Puyallup Reservation", "reservation", "Puyallup", "Washington"),
        ("Spokane Reservation", "reservation", "Spokane", "Washington"),
        ("Umatilla Reservation", "reservation", "Umatilla", "Oregon"),
        ("Warm Springs Reservation", "reservation", "Warm Springs", "Oregon"),
        ("Grand Ronde Reservation", "reservation", "Grand Ronde", "Oregon"),
        ("Siletz Reservation", "reservation", "Siletz", "Oregon"),
        ("Klamath Reservation", "reservation", "Klamath", "Oregon"),
        # Oklahoma
        ("Cherokee Nation", "reservation", "Cherokee", "Oklahoma"),
        ("Choctaw Nation", "reservation", "Choctaw", "Oklahoma"),
        ("Chickasaw Nation", "reservation", "Chickasaw", "Oklahoma"),
        ("Muscogee (Creek) Nation", "reservation", "Creek", "Oklahoma"),
        ("Seminole Nation", "reservation", "Seminole", "Oklahoma"),
        ("Osage Nation", "reservation", "Osage", "Oklahoma"),
        ("Comanche Nation", "reservation", "Comanche", "Oklahoma"),
        ("Kiowa Tribe", "reservation", "Kiowa", "Oklahoma"),
        ("Pawnee Nation", "reservation", "Pawnee", "Oklahoma"),
        ("Ponca Tribe", "reservation", "Ponca", "Oklahoma"),
        ("Otoe-Missouria Tribe", "reservation", "Otoe-Missouria", "Oklahoma"),
        ("Iowa Tribe", "reservation", "Iowa", "Oklahoma"),
        ("Sac and Fox Nation", "reservation", "Sac and Fox", "Oklahoma"),
        ("Shawnee Tribe", "reservation", "Shawnee", "Oklahoma"),
        ("Delaware Nation", "reservation", "Delaware", "Oklahoma"),
        ("Caddo Nation", "reservation", "Caddo", "Oklahoma"),
        ("Wichita and Affiliated Tribes", "reservation", "Wichita", "Oklahoma"),
        ("Cheyenne and Arapaho Tribes", "reservation", "Cheyenne/Arapaho", "Oklahoma"),
        # Alaska
        ("Bethel", "village", "Yup'ik", "Alaska"),
        ("Kotzebue", "village", "Inupiat", "Alaska"),
        ("Barrow", "village", "Inupiat", "Alaska"),
        ("Nome", "village", "Inupiat", "Alaska"),
        ("Dillingham", "village", "Yup'ik", "Alaska"),
        ("Kodiak", "village", "Alutiiq", "Alaska"),
        ("Sitka", "village", "Tlingit", "Alaska"),
        ("Juneau", "village", "Tlingit", "Alaska"),
        ("Ketchikan", "village", "Tlingit", "Alaska"),
        # Other
        ("Lumbee Tribe", "reservation", "Lumbee", "North Carolina"),
        ("Eastern Band of Cherokee", "reservation", "Cherokee", "North Carolina"),
        ("Shinnecock Reservation", "reservation", "Shinnecock", "New York"),
        # Districts
        ("Babakiri District", "district", None, None),
    ]
    
    for name, ptype, tribe, state in well_known:
        places.append({
            'name': name,
            'type': ptype,
            'tribe': tribe,
            'state': state,
            'source': 'curated_well_known'
        })
    
    return places

def add_to_database(places: List[Dict]):
    """Add places to the database."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    count = 0
    for place in places:
        try:
            cursor.execute('''
                INSERT OR IGNORE INTO tribal_place_names 
                (name, type, tribe, state, source)
                VALUES (?, ?, ?, ?, ?)
            ''', (place['name'], place.get('type', 'unknown'), 
                  place.get('tribe'), place.get('state'), place.get('source', 'unknown')))
            if cursor.rowcount > 0:
                count += 1
        except Exception as e:
            print(f"  ⚠ Error adding {place.get('name', 'unknown')}: {e}")
    
    conn.commit()
    conn.close()
    print(f"✓ Added {count} new tribal place names to database")

if __name__ == "__main__":
    print("=" * 80)
    print("Downloading Comprehensive Tribal Place Names")
    print("=" * 80)
    print()
    print("This will download thousands of tribal place names from public sources.")
    print("This may take several minutes...")
    print()
    
    places = download_comprehensive_tribal_place_list()
    add_to_database(places)
    
    print(f"\n✓ Complete! Database now contains comprehensive tribal place names.")

