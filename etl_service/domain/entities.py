# etl_service/domain/entities.py

from dataclasses import dataclass

@dataclass
class Database:
    name: str
    host: str
    port: int
    user: str = None
    password: str = None
    driver: str = None
    data_path: str = None
    log_path: str = None

    def __str__(self):
        if self.user and self.password:
            return (f"Database(name='{self.name}', host='{self.host}', port={self.port}, "
                    f"user='{self.user}', password='***', driver='{self.driver}', "
                    f"data_path='{self.data_path}', log_path='{self.log_path}')")
        else:
            return (f"Database(name='{self.name}', host='{self.host}', port={self.port}, "
                    f"Trusted_Connection=yes, driver='{self.driver}', "
                    f"data_path='{self.data_path}', log_path='{self.log_path}')")
