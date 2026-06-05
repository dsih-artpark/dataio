import yaml
import sys
import shutil
from pathlib import Path

def reformat_yaml(file_path):
    target = Path(file_path)
    if not target.exists():
        print(f"Error: {file_path} not found.")
        return

    backup_path = target.with_suffix('.yaml.backup')
    shutil.copy2(target, backup_path)
    print(f"[+] Created backup at {backup_path.name}")

    try:
        with open(target, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)
            
        with open(target, 'w', encoding='utf-8') as f:
            # Dumping with these settings naturally forces long strings into 
            # single-quoted wraps instead of using '>' block scalars
            yaml.dump(data, f, sort_keys=False, default_flow_style=False, allow_unicode=True)
            
        print(f"[+] Successfully reformatted {target.name}")
        
    except Exception as e:
        print(f"[x] Error reformatting YAML: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: uv run python reformat_yaml.py <file_path>")
        sys.exit(1)
    
    reformat_yaml(sys.argv[1])
