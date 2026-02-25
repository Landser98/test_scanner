#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Storage module for managing API projects.
Stores projects in filesystem using JSON files.
"""

from __future__ import annotations

import json
import uuid
import re
from pathlib import Path
from datetime import datetime, date
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, asdict

# --- ensure project root on sys.path ---
import sys
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import DATA_DIR


PROJECTS_DIR = DATA_DIR / "api_projects"
PROJECTS_DIR.mkdir(parents=True, exist_ok=True)


@dataclass
class Project:
    """Project data structure"""
    project_id: int
    iin: str
    create_date: datetime
    statements: List[Dict[str, Any]]  # List of statement info with status
    analytics: Dict[str, Any]  # Analytics data
    status: int  # 0=Success, 1=Failure, 2=Data mismatch
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        data = asdict(self)
        data['create_date'] = self.create_date.isoformat()
        return data
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Project':
        """Create Project from dictionary"""
        data = data.copy()
        data['create_date'] = datetime.fromisoformat(data['create_date'])
        return cls(**data)


class ProjectStorage:
    """Storage for API projects"""
    
    def __init__(self, base_dir: Path = PROJECTS_DIR):
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self._project_id_counter_file = self.base_dir / "_counter.txt"
        self._ensure_counter_file()
    
    def _ensure_counter_file(self):
        """Ensure counter file exists"""
        if not self._project_id_counter_file.exists():
            self._project_id_counter_file.write_text("1")
    
    def _get_next_project_id(self) -> int:
        """Get next project ID"""
        current = int(self._project_id_counter_file.read_text().strip() or "1")
        next_id = current + 1
        self._project_id_counter_file.write_text(str(next_id))
        return current
    
    def _validate_project_id(self, project_id: int) -> None:
        """Validate project_id to prevent path traversal"""
        if not isinstance(project_id, int) or project_id < 1:
            raise ValueError(f"Invalid project_id: {project_id}")
    
    def _sanitize_filename(self, filename: str) -> str:
        """Sanitize filename to prevent path traversal attacks"""
        # Remove path components
        filename = Path(filename).name
        # Remove dangerous characters
        filename = re.sub(r'[<>:"|?*\x00-\x1f]', '', filename)
        # Limit length
        if len(filename) > 255:
            filename = filename[:255]
        return filename
    
    def _get_project_file(self, project_id: int) -> Path:
        """Get path to project JSON file"""
        self._validate_project_id(project_id)
        # Ensure path stays within base_dir
        file_path = self.base_dir / f"project_{project_id}.json"
        # Additional check: ensure resolved path is within base_dir
        if not file_path.resolve().is_relative_to(self.base_dir.resolve()):
            raise ValueError(f"Path traversal detected: {project_id}")
        return file_path
    
    def _get_project_dir(self, project_id: int) -> Path:
        """Get directory for project files (statements)"""
        self._validate_project_id(project_id)
        # Ensure path stays within base_dir
        dir_path = self.base_dir / f"project_{project_id}_files"
        # Additional check: ensure resolved path is within base_dir
        if not dir_path.resolve().is_relative_to(self.base_dir.resolve()):
            raise ValueError(f"Path traversal detected: {project_id}")
        return dir_path
    
    def create_project(
        self,
        iin: str,
        statements: List[Dict[str, Any]],
        analytics: Dict[str, Any],
        status: int
    ) -> Project:
        """Create a new project"""
        project_id = self._get_next_project_id()
        create_date = datetime.now()
        
        project = Project(
            project_id=project_id,
            iin=iin,
            create_date=create_date,
            statements=statements,
            analytics=analytics,
            status=status
        )
        
        # Save project metadata
        from src.utils.path_security import validate_path_for_write
        project_file = self._get_project_file(project_id)
        validated = validate_path_for_write(project_file, self.base_dir)
        with open(validated, 'w', encoding='utf-8') as f:
            json.dump(project.to_dict(), f, ensure_ascii=False, indent=2)
        
        # Create directory for project files
        project_dir = self._get_project_dir(project_id)
        project_dir.mkdir(exist_ok=True)
        
        return project
    
    def get_project(self, project_id: int) -> Optional[Project]:
        """Get project by ID"""
        from src.utils.path_security import validate_path
        project_file = self._get_project_file(project_id)
        if not project_file.exists():
            return None
        validated = validate_path(project_file, self.base_dir)
        with open(validated, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        return Project.from_dict(data)
    
    def update_project(self, project: Project):
        """Update existing project"""
        from src.utils.path_security import validate_path_for_write
        project_file = self._get_project_file(project.project_id)
        validated = validate_path_for_write(project_file, self.base_dir)
        with open(validated, 'w', encoding='utf-8') as f:
            json.dump(project.to_dict(), f, ensure_ascii=False, indent=2)
    
    def get_projects_by_iin(self, iin: str) -> List[Project]:
        """Get all projects for given IIN"""
        projects = []
        
        # Scan all project files
        from src.utils.path_security import validate_path
        for project_file in self.base_dir.glob("project_*.json"):
            try:
                validated = validate_path(project_file, self.base_dir)
                with open(validated, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                if data.get('iin') == iin:
                    projects.append(Project.from_dict(data))
            except Exception:
                # Skip corrupted files
                continue
        
        # Sort by project_id (descending, newest first)
        projects.sort(key=lambda p: p.project_id, reverse=True)
        
        return projects
    
    def save_statement_file(self, project_id: int, statement_id: str, file_data: bytes, filename: str):
        """Save statement file to project directory"""
        # Validate and sanitize inputs
        self._validate_project_id(project_id)
        statement_id = self._sanitize_filename(statement_id)
        filename = self._sanitize_filename(filename)
        
        project_dir = self._get_project_dir(project_id)
        project_dir.mkdir(exist_ok=True)
        
        # Use UUID for statement_id to prevent collisions and path issues
        safe_statement_id = re.sub(r'[^a-zA-Z0-9_-]', '', statement_id)
        file_path = project_dir / f"{safe_statement_id}_{filename}"
        
        # Final check: ensure path is within project_dir
        if not file_path.resolve().is_relative_to(project_dir.resolve()):
            raise ValueError(f"Path traversal detected in filename: {filename}")
        
        file_path.write_bytes(file_data)
        
        return file_path
    
    def get_statement_files(self, project_id: int) -> List[Path]:
        """Get all statement files for a project"""
        project_dir = self._get_project_dir(project_id)
        if not project_dir.exists():
            return []
        
        return list(project_dir.glob("*.*"))


# Global storage instance
_storage = ProjectStorage()


def get_storage() -> ProjectStorage:
    """Get global storage instance"""
    return _storage

