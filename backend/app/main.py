from dataclasses import dataclass
from typing import Any, Dict, List

from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError, jwt


@dataclass
class UserContext:
    user_id: str


class ReportRepository:
    """Stub repository that emulates fetching data from an OLAP store."""

    def __init__(self) -> None:
        self._reports: Dict[str, List[Dict[str, Any]]] = {
            "demo-user": [
                {
                    "event_date": "2024-06-23",
                    "customer_id": 101,
                    "external_id": "demo-ext-101",
                    "full_name": "Demo User",
                    "signup_channel": "web",
                    "total_sessions": 14,
                    "total_session_time": 11430,
                    "avg_session_time": 816.43,
                    "errors_count": 1,
                    "total_payload_mb": 235.7,
                    "avg_signal_strength": 3.9,
                    "updated_at": "2024-06-23T21:30:00Z",
                },
                {
                    "event_date": "2024-06-24",
                    "customer_id": 101,
                    "external_id": "demo-ext-101",
                    "full_name": "Demo User",
                    "signup_channel": "web",
                    "total_sessions": 16,
                    "total_session_time": 13452,
                    "avg_session_time": 840.75,
                    "errors_count": 0,
                    "total_payload_mb": 256.1,
                    "avg_signal_strength": 4.1,
                    "updated_at": "2024-06-24T21:30:00Z",
                },
                {
                    "event_date": "2024-06-25",
                    "customer_id": 101,
                    "external_id": "demo-ext-101",
                    "full_name": "Demo User",
                    "signup_channel": "web",
                    "total_sessions": 12,
                    "total_session_time": 10812,
                    "avg_session_time": 901.0,
                    "errors_count": 2,
                    "total_payload_mb": 241.9,
                    "avg_signal_strength": 4.0,
                    "updated_at": "2024-06-25T21:30:00Z",
                },
            ]
        }

    def get_report_for_user(self, user_id: str) -> List[Dict[str, Any]]:
        if user_id not in self._reports:
            raise KeyError(user_id)
        return self._reports[user_id]


class ReportService:
    def __init__(self, repository: ReportRepository | None = None) -> None:
        self._repository = repository or ReportRepository()

    def get_user_report(self, user_id: str) -> List[Dict[str, Any]]:
        try:
            return self._repository.get_report_for_user(user_id)
        except KeyError as exc:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Report for user '{exc.args[0]}' not found",
            ) from exc


def create_app() -> FastAPI:
    app = FastAPI(title="Reports Service", version="0.1.0")

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"]
    )

    security = HTTPBearer(auto_error=True)
    report_service = ReportService()

    def get_current_user(
        credentials: HTTPAuthorizationCredentials = Depends(security),
    ) -> UserContext:
        token = credentials.credentials
        try:
            claims = jwt.get_unverified_claims(token)
        except JWTError as exc:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid authentication token",
            ) from exc

        user_id = (
            claims.get("sub")
            or claims.get("preferred_username")
            or claims.get("email")
        )
        if not user_id:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Unable to identify user in token",
            )

        return UserContext(user_id=user_id)

    @app.get("/reports")
    def get_report(user: UserContext = Depends(get_current_user)) -> List[Dict[str, Any]]:
        """Return a pre-aggregated report for the authenticated user."""
        return report_service.get_user_report(user.user_id)

    return app


app = create_app()