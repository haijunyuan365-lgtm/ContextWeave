"""Project lookup and the Phase 1 membership authorization boundary."""
from __future__ import annotations

from projects.models import Project, ProjectMember, validate_project_id
from projects.repository import ProjectRepository


class ProjectNotFound(LookupError):
    pass


class ProjectAccessDenied(PermissionError):
    pass


class ProjectService:
    def __init__(self, repository: ProjectRepository):
        self.repository = repository

    def authorize(self, project_id: str, user_id: str) -> tuple[Project, ProjectMember | None]:
        try:
            project_id = validate_project_id(project_id)
        except ValueError as exc:
            raise ProjectNotFound("project not found") from exc
        project = self.repository.get(project_id)
        if project is None or project.status != "active":
            raise ProjectNotFound("project not found")
        user_id = str(user_id or "").strip()
        if not user_id:
            raise ProjectAccessDenied("project access denied")
        member = self.repository.member(project_id, user_id)
        if project.access_mode == "closed" and (member is None or member.status != "active"):
            raise ProjectAccessDenied("project access denied")
        if member is not None and member.status != "active":
            raise ProjectAccessDenied("project access denied")
        return project, member
