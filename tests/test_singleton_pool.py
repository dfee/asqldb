import asyncio

from asphalt.core import Context
from asphalt.sqlalchemy.component import SQLAlchemyComponent
import pytest
from sqlalchemy import create_engine

from asqldemo.models import Base, Message
from asqldemo.component import SmartSQLAlchemyComponent


@pytest.fixture(scope='session')
def event_loop():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    yield loop
    loop.close()


@pytest.fixture(scope='session')
def root_ctx(event_loop):
    # This is the top level context that remains open throughout the testing
    # session
    with Context() as root_ctx:
        yield root_ctx


@pytest.fixture(scope='session')
def sqlalchemy_component(root_ctx):
    component = SmartSQLAlchemyComponent(
        debug=True,
        url='postgresql://localhost/asqldemo',
        isolation_level='AUTOCOMMIT',
    )
    root_ctx.loop.run_until_complete(component.start(root_ctx))
    sql = root_ctx.sql
    Base.metadata.create_all(root_ctx.sql.bind)
    yield
    Base.metadata.drop_all(root_ctx.sql.bind)


@pytest.fixture
def ctx(root_ctx):
    with Context(root_ctx) as ctx:
        yield ctx


@pytest.mark.parametrize(('text',), [('hello',), ('world',), ('goodnight',)])
def test_create(ctx, text, sqlalchemy_component):
    msg = Message(text=text)
    ctx.sql.add(msg)
    ctx.sql.flush()
