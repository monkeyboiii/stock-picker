"""Role-Based Access Control (RBAC) utilities"""

from enum import Enum
from typing import List


class Role(str, Enum):
    """User roles"""

    USER = "user"  # Regular user
    ANALYST = "analyst"  # Can create strategies and backtests
    ADMIN = "admin"  # Full access


# Role hierarchy: admin > analyst > user
ROLE_HIERARCHY = {
    Role.ADMIN: 3,
    Role.ANALYST: 2,
    Role.USER: 1,
}


class Permission(str, Enum):
    """Permissions for different operations"""

    # Stock data
    READ_STOCKS = "read:stocks"
    WRITE_STOCKS = "write:stocks"

    # Backtests
    READ_BACKTESTS = "read:backtests"
    WRITE_BACKTESTS = "write:backtests"
    DELETE_BACKTESTS = "delete:backtests"

    # Users
    READ_USERS = "read:users"
    WRITE_USERS = "write:users"
    DELETE_USERS = "delete:users"

    # Admin
    ADMIN_ACCESS = "admin:access"


# Role to permissions mapping
ROLE_PERMISSIONS: dict[Role, List[Permission]] = {
    Role.USER: [
        Permission.READ_STOCKS,
        Permission.READ_BACKTESTS,
    ],
    Role.ANALYST: [
        Permission.READ_STOCKS,
        Permission.WRITE_STOCKS,
        Permission.READ_BACKTESTS,
        Permission.WRITE_BACKTESTS,
        Permission.DELETE_BACKTESTS,
    ],
    Role.ADMIN: [
        # Admin has all permissions
        Permission.READ_STOCKS,
        Permission.WRITE_STOCKS,
        Permission.READ_BACKTESTS,
        Permission.WRITE_BACKTESTS,
        Permission.DELETE_BACKTESTS,
        Permission.READ_USERS,
        Permission.WRITE_USERS,
        Permission.DELETE_USERS,
        Permission.ADMIN_ACCESS,
    ],
}


def has_permission(role: str, permission: Permission) -> bool:
    """
    Check if a role has a specific permission

    Args:
        role: User's role
        permission: Permission to check

    Returns:
        True if role has permission, False otherwise
    """
    try:
        role_enum = Role(role)
        return permission in ROLE_PERMISSIONS.get(role_enum, [])
    except ValueError:
        return False


def has_role_level(user_role: str, required_role: Role) -> bool:
    """
    Check if user's role meets or exceeds required role level

    Args:
        user_role: User's current role
        required_role: Required role level

    Returns:
        True if user role meets requirement, False otherwise
    """
    try:
        user_role_enum = Role(user_role)
        user_level = ROLE_HIERARCHY.get(user_role_enum, 0)
        required_level = ROLE_HIERARCHY.get(required_role, 0)
        return user_level >= required_level
    except ValueError:
        return False


def get_role_permissions(role: str) -> List[Permission]:
    """
    Get all permissions for a specific role

    Args:
        role: User's role

    Returns:
        List of permissions
    """
    try:
        role_enum = Role(role)
        return ROLE_PERMISSIONS.get(role_enum, [])
    except ValueError:
        return []
