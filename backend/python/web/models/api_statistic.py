"""API call statistics model for tracking endpoint consumption."""

from datetime import datetime
from sqlalchemy import Column, Integer, String, Float, DateTime, Index

from web.models.base import Base


class ApiStatistic(Base):
    """
    Tracks individual API calls for consumption monitoring.
    Records endpoint, method, status code, response time, and caller info.
    """
    __tablename__ = 'api_statistics'

    id = Column(Integer, primary_key=True)
    endpoint = Column(String(255), nullable=False)
    method = Column(String(10), nullable=False)
    status_code = Column(Integer, nullable=False)
    response_time_ms = Column(Float, nullable=False)
    client_ip = Column(String(45))
    user_agent = Column(String(255))
    request_size = Column(Integer)
    response_size = Column(Integer)
    called_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    __table_args__ = (
        Index('ix_api_stats_called_at', 'called_at'),
        Index('ix_api_stats_endpoint', 'endpoint'),
        Index('ix_api_stats_endpoint_called', 'endpoint', 'called_at'),
    )

    def to_dict(self):
        return {
            'id': self.id,
            'endpoint': self.endpoint,
            'method': self.method,
            'status_code': self.status_code,
            'response_time_ms': self.response_time_ms,
            'client_ip': self.client_ip,
            'called_at': self.called_at.isoformat() if self.called_at else None,
        }

    def __repr__(self):
        return f"<ApiStatistic {self.method} {self.endpoint} {self.status_code}>"
