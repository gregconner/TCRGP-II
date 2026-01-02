# Manual Download Instructions for Thousands of Tribal Place Names

## Current Status

The automated download script now tries **ALL** alternative GNIS access methods:
1. **GNIS-LD (Linked Data)** - SPARQL endpoint (currently timing out)
2. **The National Map Services** - ArcGIS REST API (currently unavailable)
3. **GNIS FTP/Alternative URLs** - Multiple URL patterns (all unavailable)
4. **USGS GNIS National File** - Original source (503 errors)
5. **Data.gov** - ✅ Found 10 GNIS datasets available!
6. **EPA, GeoNames, etc.** - Other sources

**Current Issue**: Most GNIS servers are currently down or timing out. This is common with government data servers.

**Good News**: Data.gov API is working and found 10 GNIS datasets you can download manually!

## Manual Download Options

### Option 1: USGS GNIS National File (Recommended - 2M+ features)

1. **Visit**: https://geonames.usgs.gov/domestic/download_data.htm
2. **Download**: NationalFile.zip (contains all 2M+ geographic features)
3. **Extract**: Unzip the file
4. **Run**: The download script will automatically detect and process it if placed in the project directory

**Alternative**: Download state-specific files (smaller, more manageable):
- Visit: https://geonames.usgs.gov/domestic/download_data.htm
- Download individual state files (e.g., `AZ_Features.zip`, `NM_Features.zip`)
- Place in project directory and the script will process them

### Option 2: EPA Tribes Names Service

1. **Visit**: https://www.epa.gov/data/tribes-names-service
2. **Register**: For API access (free)
3. **Download**: Federally recognized tribes data
4. **Format**: Can be integrated into the database

### Option 3: Data.gov GNIS Datasets (✅ WORKING!)

**The script found 10 GNIS datasets on Data.gov!**

1. **Visit**: https://catalog.data.gov/dataset?q=GNIS
2. **Download**: Available datasets include:
   - GNIS Populated Places
   - Geographic Names Information System (GNIS)
   - And 8 more GNIS-related datasets
3. **Process**: Download CSV/JSON and the script can process them

**This is the most reliable option right now!**

### Option 4: GeoNames

1. **Visit**: https://www.geonames.org/export/
2. **Register**: For free API access
3. **Download**: US place names (25M+ features worldwide)
4. **Filter**: For tribal place names

## Current Database

The database currently contains:
- **119 well-known tribal places** (from curated list)
- This is a baseline - the download script will add thousands more when GNIS is available

## To Add Thousands of Places

1. **Wait for GNIS server to be available** (503 errors are temporary)
2. **Or manually download** GNIS National File and place in project directory
3. **Run**: `python3 download_tribal_places_from_sources.py`

The script will automatically:
- Detect downloaded files
- Extract tribal place names
- Add thousands of places to the database

## Expected Results

When GNIS downloads work, you should get:
- **Thousands of tribal place names** from GNIS (reservations, pueblos, villages, etc.)
- **Hundreds of federally recognized tribes** from EPA/Data.gov
- **Total: 5,000-50,000+ tribal place names** (depending on filtering criteria)

## Note

The curated list (119 places) is a good starting point, but you're absolutely right - there are literally thousands of tribal place names in the US. The download script is designed to get them all when the servers are available.

