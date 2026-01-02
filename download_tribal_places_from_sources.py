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
import time
import sys
import signal
from pathlib import Path
from typing import List, Dict
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

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
        # Data.gov is CKAN-backed. We can programmatically discover resources and download the GNIS zip
        # without requiring any API key.
        ckan_base = "https://catalog.data.gov/api/3/action"

        # Prefer the main GNIS package (USGS National Map downloadable collection).
        # If this exact query fails, we fall back to a broader search.
        preferred_queries = [
            "geographic-names-information-system-gnis-usgs-national-map-downloadable-data-collection",
            "\"Geographic Names Information System\" GNIS USGS National Map Downloadable Data Collection",
            "GNIS USGS National Map Downloadable Data Collection",
            "GNIS NationalFile.zip",
        ]

        package = None
        for q in preferred_queries:
            search_url = f"{ckan_base}/package_search?{urllib.parse.urlencode({'q': q, 'rows': 5})}"
            print(f"    Searching Data.gov CKAN for GNIS resources: {q}")
            with urllib.request.urlopen(search_url, timeout=60) as response:
                data = json.loads(response.read().decode("utf-8"))
            results = data.get("result", {}).get("results", [])
            if results:
                package = results[0]
                break

        if not package:
            print("    ⚠ Could not find GNIS packages on Data.gov via CKAN search")
            return places

        pkg_id = package.get("id") or package.get("name")
        print(f"    ✓ Found GNIS package: {package.get('title', package.get('name', 'unknown'))}")

        show_url = f"{ckan_base}/package_show?{urllib.parse.urlencode({'id': pkg_id})}"
        with urllib.request.urlopen(show_url, timeout=60) as response:
            show_data = json.loads(response.read().decode("utf-8"))

        resources = show_data.get("result", {}).get("resources", [])
        if not resources:
            print("    ⚠ GNIS package has no downloadable resources listed")
            return places

        # Pick the best downloadable resource (prefer zip).
        best = None
        for r in resources:
            url = (r.get("url") or "").strip()
            fmt = (r.get("format") or "").strip().lower()
            name = (r.get("name") or r.get("description") or "").lower()
            if not url:
                continue
            if url.lower().endswith(".zip") or fmt == "zip" or "zip" in name:
                best = r
                break

        if not best:
            # Fallback: first resource with a URL
            best = next((r for r in resources if (r.get("url") or "").strip()), None)

        if not best:
            print("    ⚠ GNIS package has no usable resource URLs")
            return places

        download_url = best.get("url").strip()
        print(f"    Downloading GNIS resource from Data.gov: {download_url}")

        # Stream download with a progress monitor (no sleeps).
        req = urllib.request.Request(download_url, headers={"User-Agent": "TCRGP-II-Downloader/1.0"})
        with urllib.request.urlopen(req, timeout=120) as response:
            total_bytes = response.headers.get("Content-Length")
            total_bytes = int(total_bytes) if total_bytes and total_bytes.isdigit() else None

            buf = io.BytesIO()
            downloaded = 0
            start = time.time()
            last_print = start

            while True:
                chunk = response.read(1024 * 256)  # 256KB chunks
                if not chunk:
                    break
                buf.write(chunk)
                downloaded += len(chunk)

                now = time.time()
                if (now - last_print) >= 1.0:
                    elapsed = max(now - start, 1e-6)
                    speed = downloaded / elapsed
                    if total_bytes:
                        pct = (downloaded / total_bytes) * 100
                        eta = int((total_bytes - downloaded) / max(speed, 1e-6))
                        sys.stdout.write(
                            f"\r    ⏬ Downloading GNIS: {pct:5.1f}% | "
                            f"{downloaded/1024/1024:8.1f}MB / {total_bytes/1024/1024:8.1f}MB | "
                            f"{speed/1024/1024:5.2f} MB/s | ETA: {eta:4d}s        "
                        )
                    else:
                        sys.stdout.write(
                            f"\r    ⏬ Downloading GNIS: {downloaded/1024/1024:8.1f}MB | "
                            f"{speed/1024/1024:5.2f} MB/s        "
                        )
                    sys.stdout.flush()
                    last_print = now

            sys.stdout.write("\n")
            sys.stdout.flush()

            data_bytes = buf.getvalue()

        # If it's a zip, parse it; otherwise attempt to parse as text.
        import zipfile
        if data_bytes[:4] == b"PK\x03\x04":
            with zipfile.ZipFile(io.BytesIO(data_bytes)) as zf:
                txt_name = next((n for n in zf.namelist() if n.lower().endswith(".txt")), None)
                if not txt_name:
                    print("    ⚠ GNIS zip had no .txt file to parse")
                    return places
                content = zf.read(txt_name).decode("utf-8", errors="ignore")
        else:
            content = data_bytes.decode("utf-8", errors="ignore")

        # GNIS format is pipe-delimited; filter for likely tribal places.
        for line in content.split("\n"):
            if not line.strip():
                continue
            parts = line.split("|")
            if len(parts) < 4:
                continue
            feature_name = parts[1].strip()
            feature_class = parts[2].strip()
            state = parts[3].strip() if len(parts) > 3 else ""

            if (
                "reservation" in feature_name.lower()
                or "pueblo" in feature_name.lower()
                or "rancheria" in feature_name.lower()
                or "tribal" in feature_name.lower()
                or feature_class in ["R", "Reservation"]
            ):
                places.append(
                    {
                        "name": feature_name,
                        "type": feature_class.lower().replace(" ", "_") if feature_class else "unknown",
                        "state": state,
                        "tribe": None,
                        "source": "datagov_gnis",
                    }
                )

        print(f"    ✓ Extracted {len(places)} likely tribal places from Data.gov GNIS resource")
                
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

def extract_place_names_from_tribal_name(name: str) -> List[str]:
    """Extract place names from tribal names.
    
    Tribal names often contain reservation names, locations, etc.
    Example: "Agua Caliente Band of Cahuilla Indians of the Agua Caliente Indian Reservation, California"
    Should extract: "Agua Caliente Indian Reservation", "California", "Agua Caliente"
    """
    places = []
    
    if not name:
        return places
    
    # Extract reservation names (common patterns)
    reservation_patterns = [
        r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s+Indian\s+Reservation',
        r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s+Reservation',
        r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s+Nation',
        r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s+Pueblo',
    ]
    
    for pattern in reservation_patterns:
        matches = re.findall(pattern, name)
        for match in matches:
            if isinstance(match, tuple):
                match = ' '.join(match)
            if match and len(match) > 2:
                places.append(match)
    
    # Extract state names at the end (after comma)
    state_match = re.search(r',\s*([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)$', name)
    if state_match:
        state = state_match.group(1)
        if state not in places:
            places.append(state)
    
    # Extract city names (often in parentheses or after comma)
    city_patterns = [
        r'\(([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\)',  # (Palm Springs)
        r',\s*([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*),\s*[A-Z]',  # , City, State
    ]
    
    for pattern in city_patterns:
        matches = re.findall(pattern, name)
        for match in matches:
            if isinstance(match, tuple):
                match = ' '.join(match)
            if match and len(match) > 2 and match not in places:
                places.append(match)
    
    return places

def download_epa_tribes_data() -> List[Dict]:
    """Download from EPA Tribes Names Service.
    
    EPA provides a Tribes Names Service with up-to-date information on
    federally recognized tribes. This is a PUBLIC API - no authentication required!
    
    We'll extract EVERYTHING:
    - All current and historical tribal names
    - All reservation names embedded in tribal names
    - All locations (states, cities, regions)
    - All place names mentioned
    
    API Documentation: https://www.epa.gov/data/tribes-names-service
    Swagger UI: https://cdxapi.epa.gov/oms-tribes-rest-services/swagger-ui/index.html
    """
    places = []
    seen_names = set()  # Track to avoid duplicates
    
    try:
        print("  Attempting to download from EPA Tribes Names Service...")
        print("    API: https://cdxapi.epa.gov/oms-tribes-rest-services/api/v1/tribes")
        print("    (Public API - no authentication required)")
        print("    Extracting ALL data: names, reservations, locations, places...")
        
        # EPA Tribes Names Service API endpoints
        base_url = "https://cdxapi.epa.gov/oms-tribes-rest-services/api/v1"
        
        # Get all tribes (basic data)
        tribes_url = f"{base_url}/tribes"
        
        print(f"    Fetching all tribes...")
        with urllib.request.urlopen(tribes_url, timeout=60) as response:
            data = json.loads(response.read().decode('utf-8'))
            
            if isinstance(data, list):
                total_tribes = len(data)
                print(f"    ✓ Got {total_tribes} tribes from EPA")
                print(f"    Fetching detailed information for each tribe...")
                print(f"    Using parallel processing (10 concurrent requests) for speed...")
                print()
                
                start_time = time.time()
                
                def fetch_tribe_details(tribe):
                    """Fetch details for a single tribe and extract all data."""
                    tribe_places = []
                    current_name = tribe.get('currentName', '')
                    epa_tribal_id = tribe.get('epaTribalInternalId')
                    
                    if not epa_tribal_id:
                        if current_name:
                            return [{
                                'name': current_name,
                                'type': 'tribe',
                                'tribe': current_name,
                                'state': None,
                                'source': 'epa_tribes_service_basic'
                            }]
                        return []
                    
                    try:
                        details_url = f"{base_url}/tribeDetails/{epa_tribal_id}"
                        with urllib.request.urlopen(details_url, timeout=15) as details_response:
                            details_data = json.loads(details_response.read().decode('utf-8'))
                            
                            if isinstance(details_data, dict):
                                details = details_data
                            elif isinstance(details_data, list) and len(details_data) > 0:
                                details = details_data[0]
                            else:
                                return []
                            
                            # Extract ALL historical names
                            names = details.get('names', [])
                            if not names:
                                names = [{'name': current_name}]
                            
                            for name_entry in names:
                                name = name_entry.get('name', '') if isinstance(name_entry, dict) else str(name_entry)
                                if name and name.strip():
                                    tribe_places.append({
                                        'name': name.strip(),
                                        'type': 'tribe',
                                        'tribe': current_name,
                                        'state': None,
                                        'source': 'epa_tribes_service'
                                    })
                                    
                                    # Extract place names
                                    extracted_places = extract_place_names_from_tribal_name(name)
                                    for place_name in extracted_places:
                                        if place_name and place_name.strip():
                                            tribe_places.append({
                                                'name': place_name.strip(),
                                                'type': 'reservation' if 'reservation' in name.lower() else 'location',
                                                'tribe': current_name,
                                                'state': None,
                                                'source': 'epa_tribes_service_extracted'
                                            })
                            
                            # Extract ALL EPA locations
                            epa_locations = details.get('epaLocations', [])
                            primary_state = None
                            for location in epa_locations:
                                state_name = location.get('stateName', '')
                                epa_region = location.get('epaRegionName', '')
                                
                                if not primary_state and state_name:
                                    primary_state = state_name
                                
                                if state_name:
                                    tribe_places.append({
                                        'name': state_name,
                                        'type': 'state',
                                        'tribe': current_name,
                                        'state': state_name,
                                        'source': 'epa_tribes_service_location'
                                    })
                                
                                if epa_region:
                                    tribe_places.append({
                                        'name': epa_region,
                                        'type': 'epa_region',
                                        'tribe': current_name,
                                        'state': state_name,
                                        'source': 'epa_tribes_service_location'
                                    })
                            
                            # Update state info
                            if primary_state:
                                for place in tribe_places:
                                    if not place.get('state'):
                                        place['state'] = primary_state
                            
                    except Exception:
                        if current_name:
                            return [{
                                'name': current_name,
                                'type': 'tribe',
                                'tribe': current_name,
                                'state': None,
                                'source': 'epa_tribes_service_basic'
                            }]
                    
                    return tribe_places
                
                # Parallel processing with REAL-TIME progress monitor (no sleeps, no threads)
                completed = 0

                # SIGALRM-based progress monitor: prints EVERY SECOND regardless of task completion.
                progress_state = {
                    "total": total_tribes,
                    "completed": 0,
                    "places": 0,
                    "start_time": start_time,
                }

                def update_progress_display():
                    """Update progress display on every completion.
                    
                    - If running in a real terminal (TTY): draw a live updating single-line monitor using \\r.
                    - If stdout is being captured (logs / Cursor tool output): emit newline progress lines
                      so updates are actually visible.
                    """
                    current_time = time.time()
                    current_completed = completed
                    current_places = len(places)
                    current_elapsed = current_time - start_time

                    if current_elapsed > 0:
                        if current_completed > 0:
                            avg_time = current_elapsed / current_completed
                            remaining = total_tribes - current_completed
                            eta_seconds = avg_time * remaining
                            eta_min = int(eta_seconds // 60)
                            eta_sec = int(eta_seconds % 60)
                            progress_pct = (current_completed / total_tribes) * 100
                            speed = current_completed / current_elapsed
                        else:
                            progress_pct = 0.0
                            speed = 0.0
                            eta_min = 0
                            eta_sec = 0

                        elapsed_min = int(current_elapsed // 60)
                        elapsed_sec = int(current_elapsed % 60)

                        progress_msg = (
                            f"    ⏳ PROGRESS: [{current_completed:4d}/{total_tribes}] "
                            f"({progress_pct:5.1f}%) | "
                            f"Places: {current_places:5d} | "
                            f"Speed: {speed:5.2f} tribes/sec | "
                            f"Elapsed: {elapsed_min:2d}m {elapsed_sec:2d}s | "
                            f"ETA: {eta_min:2d}m {eta_sec:2d}s"
                        )

                        if sys.stdout.isatty():
                            sys.stdout.write("\r" + progress_msg + "        ")
                            sys.stdout.flush()
                        else:
                            sys.stdout.write(progress_msg + "\n")
                            sys.stdout.flush()
                    else:
                        start_msg = f"    ⏳ PROGRESS: [   0/{total_tribes}] (  0.0%) | Starting..."
                        if sys.stdout.isatty():
                            sys.stdout.write("\r" + start_msg + "        ")
                            sys.stdout.flush()
                        else:
                            sys.stdout.write(start_msg + "\n")
                            sys.stdout.flush()

                def _progress_tick(_signum, _frame):
                    """Emit progress once per second (no sleep)."""
                    try:
                        now = time.time()
                        total = progress_state["total"]
                        done = progress_state["completed"]
                        places_n = progress_state["places"]
                        elapsed = max(now - progress_state["start_time"], 1e-6)
                        pct = (done / total) * 100 if total else 100.0
                        speed = done / elapsed if elapsed > 0 else 0.0
                        if done > 0:
                            avg = elapsed / done
                            eta = int(avg * max(total - done, 0))
                        else:
                            eta = 0
                        eta_min, eta_sec = divmod(eta, 60)
                        elapsed_min, elapsed_sec = divmod(int(elapsed), 60)

                        msg = (
                            f"    ⏳ PROGRESS: [{done:4d}/{total}] "
                            f"({pct:5.1f}%) | "
                            f"Places: {places_n:5d} | "
                            f"Speed: {speed:5.2f} tribes/sec | "
                            f"Elapsed: {elapsed_min:2d}m {elapsed_sec:2d}s | "
                            f"ETA: {eta_min:2d}m {eta_sec:2d}s"
                        )

                        if sys.stdout.isatty():
                            sys.stdout.write("\r" + msg + "        ")
                        else:
                            sys.stdout.write(msg + "\n")
                        sys.stdout.flush()
                    except Exception:
                        return

                prev_alarm_handler = signal.getsignal(signal.SIGALRM)
                signal.signal(signal.SIGALRM, _progress_tick)
                signal.setitimer(signal.ITIMER_REAL, 1.0, 1.0)

                try:
                    with ThreadPoolExecutor(max_workers=10) as executor:
                        future_to_tribe = {executor.submit(fetch_tribe_details, tribe): tribe for tribe in data}
                        
                        for future in as_completed(future_to_tribe):
                            completed += 1
                            try:
                                tribe_places = future.result()
                                for place in tribe_places:
                                    name_lower = place['name'].lower().strip()
                                    if name_lower not in seen_names:
                                        seen_names.add(name_lower)
                                        places.append(place)
                            except Exception:
                                pass
                            
                            # Update progress display immediately after each completion
                            progress_state["completed"] = completed
                            progress_state["places"] = len(places)
                            update_progress_display()
                
                finally:
                    # Stop per-second alarm updates
                    try:
                        signal.setitimer(signal.ITIMER_REAL, 0)
                        signal.signal(signal.SIGALRM, prev_alarm_handler)
                    except Exception:
                        pass

                    # Final progress display
                    elapsed_total = time.time() - start_time
                    sys.stdout.write(f"\r    ✓ COMPLETE: [{total_tribes}/{total_tribes}] "
                                   f"(100.0%) | "
                                   f"Places: {len(places):5d} | "
                                   f"Time: {int(elapsed_total//60)}m {int(elapsed_total%60)}s"
                                   f"                    \n")
                    sys.stdout.flush()
                
                print(f"    ✓ Extracted {len(places)} tribal place names from EPA")
                print(f"    ✓ Includes: tribal names, reservations, locations, states, regions")
                
    except urllib.error.URLError as e:
        print(f"    ⚠ Could not access EPA Tribes Names Service: {e}")
    except Exception as e:
        print(f"    ⚠ Error accessing EPA Tribes Names Service: {e}")
        import traceback
        traceback.print_exc()
    
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
    """Try alternative sources for tribal place names.
    
    Note: EPA Tribes Names Service is already called separately, so we skip it here.
    """
    places = []
    
    print("  Trying other alternative sources...")
    
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

    # Try Data.gov FIRST (CKAN resources often stay available even when GNIS endpoints are flaky)
    print("\n1. Downloading GNIS from Data.gov (CKAN resources)...")
    datagov_places = download_datagov_gnis()
    if datagov_places:
        print(f"  ✓ Got {len(datagov_places)} places from Data.gov")
        all_places.extend(datagov_places)

    # Then try GNIS-LD / National Map / GNIS direct (often down)
    print("\n2. Attempting to download from GNIS-LD (Linked Data) service...")
    gnis_ld_places = download_gnis_ld_sparql()
    if gnis_ld_places:
        print(f"  ✓ Got {len(gnis_ld_places)} places from GNIS-LD")
        all_places.extend(gnis_ld_places)

    print("\n3. Attempting to download from The National Map Services...")
    national_map_places = download_national_map_api()
    if national_map_places:
        print(f"  ✓ Got {len(national_map_places)} places from The National Map")
        all_places.extend(national_map_places)

    # Try original GNIS National File (may be down)
    print("\n4. Attempting to download from USGS GNIS National File...")
    gnis_places = download_gnis_national_file()
    if gnis_places:
        print(f"  ✓ Got {len(gnis_places)} places from GNIS")
        all_places.extend(gnis_places)
    else:
        print("  → GNIS National File unavailable (server may be down)")
    
    # Try EPA Tribes Names Service (PUBLIC API - no auth required!)
    print("\n5. Downloading from EPA Tribes Names Service (PUBLIC API)...")
    epa_places = download_epa_tribes_data()
    if epa_places:
        print(f"  ✓ Got {len(epa_places)} places from EPA Tribes Names Service")
        all_places.extend(epa_places)
    
    # Try other alternative sources
    print("\n6. Trying other alternative sources...")
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

