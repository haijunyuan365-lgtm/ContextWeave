"""Validated, immutable project domain objects."""
from __future__ import annotations

import re
from dataclasses import asdict, dataclass, field
from typing import Any


PROJECT_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$")
PROJECT_ROLES = {"platform_admin", "project_admin", "support_staff", "customer"}
AGENT_TYPES = {"general", "technical", "billing", "escalation"}


def validate_project_id(project_id: str) -> str:
    value = str(project_id or "").strip()
    if not PROJECT_ID_PATTERN.fullmatch(value):
        raise ValueError("project_id must be 1-64 characters using letters, numbers, '_' or '-'")
    return value


@dataclass(frozen=True)
class ProjectConfig:
    system_prompt: str = ""
    allowed_agents: tuple[str, ...] = ("general", "technical", "billing", "escalation")
    allowed_skills: tuple[str, ...] = ()
    allowed_tools: tuple[str, ...] = ()
    model_config: dict[str, Any] = field(default_factory=dict)
    handoff_policy: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        invalid = set(self.allowed_agents) - AGENT_TYPES
        if invalid:
            raise ValueError(f"unsupported agent types: {sorted(invalid)}")
        if not self.allowed_agents:
            raise ValueError("allowed_agents cannot be empty")

    @classmethod
    def from_dict(cls, data: dict[str, Any] | None) -> "ProjectConfig":
        raw = dict(data or {})
        default_agents = ("general", "technical", "billing", "escalation")
        allowed_agents = raw["allowed_agents"] if "allowed_agents" in raw else default_agents
        return cls(
            system_prompt=str(raw.get("system_prompt") or ""),
            allowed_agents=tuple(allowed_agents),
            allowed_skills=tuple(raw.get("allowed_skills") or ()),
            allowed_tools=tuple(raw.get("allowed_tools") or ()),
            model_config=dict(raw.get("model_config") or {}),
            handoff_policy=dict(raw.get("handoff_policy") or {}),
        )


@dataclass(frozen=True)
class Project:
    id: str
    name: str
    status: str = "active"
    access_mode: str = "closed"
    config: ProjectConfig = field(default_factory=ProjectConfig)

    def __post_init__(self) -> None:
        object.__setattr__(self, "id", validate_project_id(self.id))
        if not self.name.strip():
            raise ValueError("project name cannot be empty")
        if self.status not in {"active", "disabled"}:
            raise ValueError("project status must be active or disabled")
        if self.access_mode not in {"open", "closed"}:
            raise ValueError("project access_mode must be open or closed")

    def public_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ProjectMember:
    project_id: str
    user_id: str
    role: str = "customer"
    status: str = "active"

    def __post_init__(self) -> None:
        object.__setattr__(self, "project_id", validate_project_id(self.project_id))
        if not self.user_id.strip():
            raise ValueError("user_id cannot be empty")
        if self.role not in PROJECT_ROLES:
            raise ValueError(f"unsupported project role: {self.role}")
        if self.status not in {"active", "disabled"}:
            raise ValueError("member status must be active or disabled")
