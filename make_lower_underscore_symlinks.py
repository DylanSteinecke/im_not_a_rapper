#!/usr/bin/env python3
"""
Makes symbolic links that are lower_case_underscores instead of Spaces And/Capitalized Folders
"""
from collections import defaultdict
import os
import re
import sys
from pathlib import Path


def make_symlinks(base_dir: str):
    """
    Recursively create lowercase, underscore-based symlinks for
    files and directories for paths that have spaces or capitals
    """
    try:
        base_path = Path(base_dir).resolve(strict=True)
        print(f"Scanning {base_path}...")
    except FileNotFoundError:
        print(f"Error: Base directory not found: {base_dir}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error resolving path: {e}", file=sys.stderr)
        sys.exit(1)


    # Walk through the directory tree
    collisions = defaultdict(list)  # (parent_dir, new_name) -> [originals...]
    for dirpath_str, dirnames, filenames in os.walk(str(base_path), topdown=True):
        
        # Exclude the hidden directories from traversal
        dirnames[:] = [
            dirname for dirname in dirnames if not dirname.startswith(".")]
        
        # Iterate through all non-hidden dirs and files 
        current_dir_path = Path(dirpath_str)
        for sub_name in [n for n in dirnames] + [n for n in filenames if not n.startswith('.')]:    

            # Get old path (to be linked to) and new path (symlink)
            old_path = current_dir_path / sub_name
            new_path_name = re.sub(r"\s+", "_", sub_name.lower())
            
            # Skip if the path is already normalized or is a symlink
            if new_path_name == sub_name or old_path.is_symlink():
                continue 
            collisions[(current_dir_path, new_path_name)].append(sub_name)

            # Assign new path name
            new_path = old_path.with_name(new_path_name)

            # Skip if the path exists already 
            if os.path.lexists(new_path):
                print(f"Skipping existing: {new_path.relative_to(base_path)}")
                continue

            # Create the symlink from the new path to the old path
            try:
                rel_path = os.path.relpath(old_path, new_path.parent)
                if os.name == "nt" and old_path.is_dir(): # Windows
                    new_path.symlink_to(rel_path, target_is_directory=True)
                else: 
                    new_path.symlink_to(rel_path)
                print(f"Linked: {new_path.relative_to(base_path)} -> {rel_path}")

            except OSError as e:
                print(f"Failed to link {new_path.relative_to(base_path)}: {e}", 
                      file=sys.stderr)
            except NotImplementedError:
                print(f"Symlinks not supported on this system."\
                      f"Skipping {new_path.relative_to(base_path)}", 
                      file=sys.stderr)
                return
        
    for (parent_dir, new_name), originals in collisions.items():
        if len(originals) > 1:
            print(f"Name collision in {Path(parent_dir).relative_to(base_path)}: "
                f"{originals} -> {new_name}", file=sys.stderr)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} /path/to/base_dir", file=sys.stderr)
        sys.exit(1)

    make_symlinks(sys.argv[1])
