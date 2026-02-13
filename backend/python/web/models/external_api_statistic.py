"""External (outbound) API call statistics model."""

from datetime import datetime
from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, Index

from web.models.base import Base


class ExternalApiStatistic(Base):
    """
    Tracks outbound API calls to external services (SOAP, SugarCRM, etc.).
    Records service, endpoint/operation, method, status, timing, and sizes.
    """
    __tablename__ = 'external_api_statistics'

    id = Column(Integer, primary_key=True)
    service_name = Column(String(50), nullable=False)
    endpoint = Column(String(500), nullable=False)
    method = Column(String(10), nullable=False)
    status_code = Column(Integer)
    response_time_ms = Column(Float, nullable=False)
    request_size = Column(Integer)
    response_size = Column(Integer)
    success = Column(Boolean, nullable=False, default=True)
    error_message = Column(String(500))
    caller = Column(String(100))
    called_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    __table_args__ = (
        Index('ix_ext_api_called_at', 'called_at'),
        Index('ix_ext_api_service', 'service_name'),
        Index('ix_ext_api_service_called', 'service_name', 'called_at'),
    )

    def to_dict(self):
        return {
            'id': self.id,
            'service_name': self.service_name,
            'endpoint': self.endpoint,
            'method': self.method,
            'status_code': self.status_code,
            'response_time_ms': self.response_time_ms,
            'request_size': self.request_size,
            'response_size': self.response_size,
            'success': self.success,
            'error_message': self.error_message,
            'caller': self.caller,
            'called_at': self.called_at.isoformat() if self.called_at else None,
        }

    def __repr__(self):
        return f"<ExternalApiStatistic {self.service_name} {self.method} {self.endpoint}>"
