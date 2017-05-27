from typing import Optional

from asphalt.core import Context
from asphalt.sqlalchemy.component import SQLAlchemyComponent
from asyncio_extras.threads import call_in_executor
from sqlalchemy.orm import (
    Session,
    sessionmaker,
)

class SingletonSession:
    registry = {}

    def __init__(self, factory, ctx):
        self.factory = factory
        self.info = dict(self.session.info, ctx=ctx)
        #  self._transaction = self.session.begin(subtransactions=True)

    def __new__(cls, factory, ctx, *args, **kwargs):
        ins = super().__new__(cls)
        if factory not in cls.registry:
            ins.session = cls.registry[factory] = factory()
        else:
            ins.session = cls.registry[factory]
        ins._transaction = ins.session.begin(subtransactions=True)
        return ins

    def __getattr__(self, name):
        return getattr(self.session, name)

    def close(self):
        pass


class SmartSQLAlchemyComponent(SQLAlchemyComponent):
    def __init__(self, *args, **kwargs):
        self.debug = kwargs.pop('debug', False)
        super().__init__(*args, **kwargs)

    def create_session(self, ctx: Context, factory: sessionmaker) -> Session:
        async def teardown_session(exception: Optional[BaseException]) -> None:
            try:
                if exception is None and session.is_active:
                    await call_in_executor(
                        session.commit,
                        executor=self.commit_executor,
                    )
            finally:
                del session.info['ctx']
                session.close()

        session = SingletonSession(factory, ctx) if self.debug \
            else factory(info={'ctx': ctx})

        ctx.add_teardown_callback(teardown_session, pass_exception=True)
        return session
