"""
S3 services for Consultor Turbo
"""

from .s3_metadata_service import ConsultorTurboS3MetadataService
from .s3_comparator_service import ConsultorTurboS3ComparatorService

__all__ = [
    'ConsultorTurboS3MetadataService',
    'ConsultorTurboS3ComparatorService'
]

