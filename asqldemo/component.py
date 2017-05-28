from typing import Optional

from asphalt.core import Context
from asphalt.sqlalchemy.component import SQLAlchemyComponent
from asyncio_extras.threads import call_in_executor
from sqlalchemy.orm import (
    Session,
    sessionmaker,
)

from .db import SingletonSession


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
