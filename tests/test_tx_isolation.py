import asyncio

from asphalt.core import Context
from asphalt.sqlalchemy.component import SQLAlchemyComponent
import pytest
from sqlalchemy import event
from sqlalchemy.orm import sessionmaker

from asqldemo.db import SingletonSession
from asqldemo.models import (
    Base,
    Message,
)


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


@pytest.fixture
def ctx(root_ctx):
    with Context(root_ctx) as ctx:
        yield ctx


def test_create(ctx, sqlalchemy_component):
    with Context(ctx) as subctx:
        msg = Message(text='hello world')
        subctx.sql.add(msg)
        subctx.sql.flush()
        assert ctx.sql.query(Message).count() == 1
    assert ctx.sql.query(Message).count() == 0


def test_mapper_event(ctx, sqlalchemy_component):
    is_called = False
    def call_after_insert(mapper, connection, target):
        nonlocal is_called
        is_called = True
    event.listen(Message, 'after_insert', call_after_insert)
    with Context(ctx) as subctx:
        msg = Message(text='hello world')
        subctx.sql.add(msg)
        subctx.sql.flush()
    event.remove(Message, 'after_insert', call_after_insert)
    assert is_called


def test_session_event(ctx, sqlalchemy_component):
    is_called = False
    def before_commit_hook(session):
        nonlocal is_called
        is_called = True
    factory = ctx.require_resource(sessionmaker)
    event.listen(factory, 'before_commit', before_commit_hook)
    with Context(ctx) as subctx:
        msg = Message(text='hello world')
        subctx.sql.add(msg)
        subctx.sql.flush()
        subctx.sql.commit()
    event.remove(factory, 'before_commit', before_commit_hook)
    assert is_called
