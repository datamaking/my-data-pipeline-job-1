from typing import Dict, Any, List, Optional, Union
from ..base_config import BaseConfig, ConfigSingleton


class RDBMSSourceConfig(BaseConfig):
    """Configuration for RDBMS source systems."""
    
    def __init__(
        self,
        jdbc_url: str,
        driver: str,
        user: str,
        password: str,
        tables: Optional[List[str]] = None,
        queries: Optional[List[str]] = None,
        options: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize RDBMS source configuration.
        
        Args:
            jdbc_url: JDBC URL for the database
            driver: JDBC driver class
            user: Database username
            password: Database password
            tables: List of tables to read from
            queries: List of SQL queries to execute
            options: Additional options for database connection
        """
        self.jdbc_url = jdbc_url
        self.driver = driver
        self.user = user
        self.password = password
        self.tables = tables or []
        self.queries = queries or []
        self.options = options or {}
        
        if not self.tables and not self.queries:
            raise ValueError("Either tables or queries must be provided")
    
    def get_config(self) -> Dict[str, Any]:
        """Get the configuration as a dictionary."""
        return {
            "jdbc_url": self.jdbc_url,
            "driver": self.driver,
            "user": self.user,
            "password": self.password,
            "tables": self.tables,
            "queries": self.queries,
            "options": self.options
        }
    
    def validate(self) -> bool:
        """Validate the configuration."""
        if not self.jdbc_url or not self.driver:
            return False
        if not self.user or not self.password:
            return False
        if not self.tables and not self.queries:
            return False
        return True


# Example usage:
# rdbms_config = ConfigSingleton.get_instance(
#     RDBMSSourceConfig,
#     jdbc_url="jdbc:postgresql://localhost:5432/mydatabase",
#     driver="org.postgresql.Driver",
#     user="username",
#     password="password",
#     tables=["table1", "table2"],
#     options={"fetchsize": 10000}
# )