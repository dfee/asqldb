from sqlalchemy import (
    Column,
    Integer,
    String,
    event,
)
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class Message(Base):
    __tablename__ = 'messages'

    id = Column(Integer, primary_key=True)
    text = Column(String)
