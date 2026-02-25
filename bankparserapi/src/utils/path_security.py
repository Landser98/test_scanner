"""
Security utilities for path validation to prevent path traversal attacks.
"""
from pathlib import Path
from typing import Union


def validate_path_for_write(path: Union[str, Path], base_dir: Union[str, Path, None] = None) -> Path:
    """Validate path for write - path may not exist yet. Ensures it stays within base_dir."""
    resolved = Path(path).resolve()
    if base_dir is not None:
        base_resolved = Path(base_dir).resolve()
        try:
            if not resolved.is_relative_to(base_resolved):
                raise ValueError(f"Path traversal detected: {path}")
        except AttributeError:
            try:
                resolved.relative_to(base_resolved)
            except ValueError:
                raise ValueError(f"Path traversal detected: {path}")
    return resolved


def validate_path(path: Union[str, Path], base_dir: Union[str, Path, None] = None) -> Path:
    """
    Validate and resolve a path, ensuring it doesn't escape base_dir.
    
    Args:
        path: Path to validate
        base_dir: Base directory that path must be within (optional)
    
    Returns:
        Resolved Path object
    
    Raises:
        ValueError: If path is invalid or escapes base_dir
    """
    resolved_path = Path(path).resolve()
    
    # Check if path exists and is valid
    if not resolved_path.exists():
        raise ValueError(f"Path does not exist: {path}")
    
    # If base_dir is specified, ensure path is within it
    if base_dir is not None:
        base_resolved = Path(base_dir).resolve()
        try:
            if not resolved_path.is_relative_to(base_resolved):
                raise ValueError(f"Path traversal detected: {path} is not within {base_dir}")
        except AttributeError:
            # Python < 3.9 compatibility
            try:
                resolved_path.relative_to(base_resolved)
            except ValueError:
                raise ValueError(f"Path traversal detected: {path} is not within {base_dir}")
    
    return resolved_path


def sanitize_filename(filename: str, max_length: int = 255) -> str:
    """
    Sanitize filename to prevent path traversal and injection attacks.
    
    Args:
        filename: Filename to sanitize
        max_length: Maximum length of filename
    
    Returns:
        Sanitized filename
    """
    import re
    
    # Remove path components
    filename = Path(filename).name
    
    # Remove dangerous characters
    filename = re.sub(r'[<>:"|?*\x00-\x1f]', '', filename)
    
    # Remove leading/trailing dots and spaces
    filename = filename.strip('. ')
    
    # Limit length
    if len(filename) > max_length:
        filename = filename[:max_length]
    
    # Ensure filename is not empty
    if not filename:
        filename = "file"
    
    return filename


def safe_open_file(file_path: Union[str, Path], mode: str = 'r', 
                   base_dir: Union[str, Path, None] = None, **kwargs):
    """
    Safely open a file with path validation.
    Path is validated before open() to prevent resource injection.
    
    Args:
        file_path: Path to file
        mode: File open mode
        base_dir: Base directory that path must be within (optional)
        **kwargs: Additional arguments for open()
    
    Returns:
        File object
    
    Raises:
        ValueError: If path is invalid or escapes base_dir
    """
    validated_path = validate_path(file_path, base_dir)
    return open(validated_path, mode, **kwargs)
