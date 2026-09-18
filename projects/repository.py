"""File-backed project configuration loaded once at application startup."""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from projects.models import Project, ProjectConfig, ProjectMember


class ProjectRepository:
    def __init__(self, projects: list[Project], members: list[ProjectMember] | None = None):
        self._projects = {project.id: project for project in projects}
        if len(self._projects) != len(projects):
            raise ValueError("duplicate project id")
        self._members = {(member.project_id, member.user_id): member for member in (members or [])}
        if any(project_id not in self._projects for project_id, _ in self._members):
            raise ValueError("project member references an unknown project")

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ProjectRepository":
        if not isinstance(data, dict) or not isinstance(data.get("projects"), list):
            raise ValueError("project config must contain a projects array")
        projects = [Project(
            id=item["id"],
            name=item["name"],
            status=item.get("status", "active"),
            access_mode=item.get("access_mode", "closed"),
            config=ProjectConfig.from_dict(item.get("config")),
        ) for item in data["projects"]]
        members = [ProjectMember(**item) for item in data.get("members", [])]
        return cls(projects, members)

    @classmethod
    def from_file(cls, path: str | Path) -> "ProjectRepository":
        return cls.from_dict(json.loads(Path(path).read_text(encoding="utf-8")))

    def get(self, project_id: str) -> Project | None:
        return self._projects.get(project_id)

    def member(self, project_id: str, user_id: str) -> ProjectMember | None:
        return self._members.get((project_id, user_id))

    def projects(self) -> list[Project]:
        return list(self._projects.values())


def load_project_repository(path: str | Path | None = None) -> ProjectRepository:
    configured = path or os.getenv("ECHOMIND_PROJECTS_CONFIG", "").strip()
    target = Path(configured) if configured else Path(__file__).resolve().parent.parent / "config" / "projects.json"
    return ProjectRepository.from_file(target)
