import asyncio

from asphalt.core import Context
from asphalt.sqlalchemy.component import SQLAlchemyComponent
import pytest
from sqlalchemy import create_engine

from asqldemo.db import SingletonSession
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
    component = SQLAlchemyComponent(
        url='postgresql://localhost/asqldemo',
        ready_callback=SingletonSession.register,
    )
    root_ctx.loop.run_until_complete(component.start(root_ctx))
    Base.metadata.create_all(root_ctx.sql.bind)
    sql = root_ctx.sql
    #  import ipdb; ipdb.set_trace()
    yield
    sql.close()


@pytest.fixture
def ctx(root_ctx):
    with Context(root_ctx) as ctx:
        yield ctx


@pytest.mark.parametrize(('text',), [
    ('hello',),
    ('world',),
    ('goodnight',),
])
def test_create(ctx, text, sqlalchemy_component):
    assert ctx.sql.query(Message).count() == 0
    with Context(ctx) as subctx:
        msg = Message(text=text)
        subctx.sql.add(msg)
        subctx.sql.flush()
    assert ctx.sql.query(Message).count() == 0
