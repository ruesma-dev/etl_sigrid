# etl_service/domain/interfaces.py
from abc import ABC, abstractmethod
from typing import List
from etl_service.domain.repositories_interfaces import BaseTransformation

class SpecificTableTransformations(ABC):
    @abstractmethod
    def get_table_transformations(self) -> List[BaseTransformation]:
        """Devuelve una lista de transformaciones ETL específicas de la tabla."""
        pass
