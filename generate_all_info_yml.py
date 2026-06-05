import sys
import yaml
import re
from pathlib import Path

def generate_rds_id(meta):
    """Automatically derive the RDS ID from the collection ID or dataset ID."""
    
    # 1. Try to get it from collection: ID
    collection = meta.get("collection", {})
    col_id = collection.get("ID", "")
    
    if col_id:
        match = re.match(r"([a-zA-Z]+)(\d+)", str(col_id))
        if match:
            letters = match.group(1).upper()
            digits = str(int(match.group(2))) # Strips leading zeros (e.g., 0007 -> 7)
            return f"{letters}RDS{digits}"

    # 2. Fallback: Try to extract it from the Dataset ID (e.g., CS0007DS0113)
    dataset_id = str(meta.get("datasetID", ""))
    if dataset_id:
        match = re.match(r"([a-zA-Z]+)(\d+)DS", dataset_id.upper())
        if match:
            letters = match.group(1)
            digits = str(int(match.group(2)))
            return f"{letters}RDS{digits}"
            
    return ""

def generate_info_yml(folder_path):
    folder = Path(folder_path)
    
    if not folder.exists() or not folder.is_dir():
        print(f"Error: Directory '{folder_path}' not found.")
        return

    metadata_path = folder / "metadata.yaml"
    if not metadata_path.exists():
        print(f"Error: metadata.yaml not found in '{folder_path}'.")
        return

    try:
        with open(metadata_path, 'r', encoding='utf-8') as f:
            meta = yaml.safe_load(f)
        
        if not meta:
            print("Error: metadata.yaml is empty.")
            return
        
        # Extract fields
        title = meta.get("datasetTitle", meta.get("title", ""))
        data_owner_name = meta.get("datasetOwner", meta.get("data_owner_name", ""))
        
        # Clean up description
        description = meta.get("datasetDescription", meta.get("description", ""))
        if description:
            description = description.replace('\n', ' ').replace('\\', '').strip()
            # Clean up double spaces
            while "  " in description:
                description = description.replace("  ", " ")
        
        # Parse temporal coverage
        temporal_coverage = str(meta.get("temporalCoverage", meta.get("temporal_coverage", "")))
        start_date = temporal_coverage
        end_date = temporal_coverage
        if "-" in temporal_coverage:
            parts = temporal_coverage.split("-")
            start_date = parts[0].strip()
            end_date = parts[1].strip()
        elif " to " in temporal_coverage:
            parts = temporal_coverage.split(" to ")
            start_date = parts[0].strip()
            end_date = parts[1].strip()

        # Format temporal resolution
        temporal_resolution = str(meta.get("temporalResolution", "YEAR")).upper()
        
        # Extract source
        source_field = meta.get("source", [])
        if isinstance(source_field, list):
            source_str = " ".join([str(s) for s in source_field])
        else:
            source_str = str(source_field)

        # Automatically generate RDS ID
        generated_rds_id = generate_rds_id(meta)

        # Build info.yml dictionary
        info = {
            "title": title,
            "data_owner_name": data_owner_name,
            "description": description,
            "temporal_coverage_start_date": start_date,
            "temporal_coverage_end_date": end_date,
            "temporal_resolution": temporal_resolution,
            "access_level": "DOWNLOAD",
            "raw_dataset": {
                "rds_id": generated_rds_id,
                "source": source_str
            }
        }

        info_path = folder / "info.yml"
        with open(info_path, 'w', encoding='utf-8') as f:
            yaml.dump(info, f, sort_keys=False, default_flow_style=False, allow_unicode=True)
        
        print(f"[\u2713] Successfully generated info.yml in {folder.name} (Auto-generated rds_id: '{generated_rds_id}')")
        
    except Exception as e:
        print(f"[x] Error processing {folder.name}: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: uv run python generate_all_info_yml.py <path_to_dataset_folder>")
        sys.exit(1)
    
    dataset_folder = sys.argv[1]
    generate_info_yml(dataset_folder)
