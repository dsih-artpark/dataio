from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from dataio.validate.contracts.models import DatasetManifest, ValidationRequest
from dataio.validate.reports.models import ValidationResult


class ValidatorPlugin(ABC):
    @abstractmethod
    def supports(self, request: ValidationRequest) -> bool:
        raise NotImplementedError

    @abstractmethod
    def validate_structure(
        self,
        manifest: DatasetManifest,
        data: Any,
        request: ValidationRequest,
        result: ValidationResult,
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    def validate_metadata(
        self,
        manifest: DatasetManifest,
        request: ValidationRequest,
        result: ValidationResult,
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    def validate_content(
        self,
        manifest: DatasetManifest,
        data: Any,
        request: ValidationRequest,
        result: ValidationResult,
    ) -> None:
        raise NotImplementedError
