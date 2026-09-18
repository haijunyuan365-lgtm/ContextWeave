import json

import pytest

from projects.models import Project, ProjectConfig, ProjectMember
from projects.repository import ProjectRepository
from projects.service import ProjectAccessDenied, ProjectNotFound, ProjectService


def repository():
    return ProjectRepository(
        projects=[
            Project("default", "Default", access_mode="open"),
            Project("project-a", "Project A", config=ProjectConfig(system_prompt="A only")),
            Project("project-b", "Project B"),
            Project("disabled", "Disabled", status="disabled"),
        ],
        members=[
            ProjectMember("project-a", "alice", "customer"),
            ProjectMember("project-b", "bob", "project_admin"),
            ProjectMember("project-a", "blocked", status="disabled"),
        ],
    )


def test_project_service_allows_open_project_and_active_member():
    service = ProjectService(repository())
    default, default_member = service.authorize("default", "any-user")
    project, member = service.authorize("project-a", "alice")
    assert default.id == "default" and default_member is None
    assert project.config.system_prompt == "A only"
    assert member.role == "customer"


@pytest.mark.parametrize("project_id,user_id,error", [
    ("missing", "alice", ProjectNotFound),
    ("disabled", "alice", ProjectNotFound),
    ("project-a", "bob", ProjectAccessDenied),
    ("project-a", "blocked", ProjectAccessDenied),
    ("project-a", "", ProjectAccessDenied),
])
def test_project_service_rejects_invalid_access(project_id, user_id, error):
    with pytest.raises(error):
        ProjectService(repository()).authorize(project_id, user_id)


def test_repository_loads_validated_json(tmp_path):
    path = tmp_path / "projects.json"
    path.write_text(json.dumps({
        "projects": [{"id": "p1", "name": "Project 1"}],
        "members": [{"project_id": "p1", "user_id": "u1", "role": "support_staff"}],
    }), encoding="utf-8")
    repo = ProjectRepository.from_file(path)
    assert repo.get("p1").access_mode == "closed"
    assert repo.member("p1", "u1").role == "support_staff"


def test_project_config_rejects_unknown_agent():
    with pytest.raises(ValueError, match="unsupported agent"):
        ProjectConfig(allowed_agents=("general", "invented"))


def test_project_config_rejects_explicit_empty_agent_list():
    with pytest.raises(ValueError, match="cannot be empty"):
        ProjectConfig.from_dict({"allowed_agents": []})
