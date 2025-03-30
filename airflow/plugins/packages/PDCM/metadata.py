

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, DateTime, Boolean

# # Configure the connection pool
# def get_engine():
#     db_url = "postgresql://pdcm:pdcm@pdcmmetastore_container:5432/pdcm"
#     engine = create_engine(db_url, pool_size=10, max_overflow=5, echo=False)
#     return engine

# # Create a session maker using the pooled engine
# engine = get_engine()
# SessionLocal = sessionmaker(bind=engine)
# ------------------ SQLAlchemy Setup ------------------ #
Base = declarative_base()


class FileMetadata(Base):
    """
    Stores robust metadata about each file:
        - file_path: unique path or identifier (primary key)
        - last_modified: last modification time
        - file_hash: e.g., MD5 or other hash for detecting changes
        - is_deleted: True if no longer valid on disk
    """
    __tablename__ = 'file_metadata'
    
    file_path = Column(String, primary_key=True)
    last_modified = Column(DateTime, nullable=False)
    file_hash = Column(String, nullable=False)
    is_deleted = Column(Boolean, default=False)
    cik = Column(String, nullable=False)