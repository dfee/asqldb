from sqlalchemy.orm import Session


class SingletonSession(Session):
    registry = {}

    def __init__(self, *args, bind, **kwargs):
        connection = self.registry[bind][0]
        self._tx = connection.begin_nested()
        super().__init__(*args, bind=connection, **kwargs)

    def close(self):
        super().close()
        self._tx.rollback()

    @property
    def tx_chain(self):
        def get_chain(tx):
            return [*get_chain(tx.parent), tx] if tx.parent else [tx]
        return get_chain(self.transaction)

    @classmethod
    def register(cls, engine, maker):
        connection = engine.connect()
        root_tx = connection.begin()
        cls.registry[engine] = (connection, root_tx)
        maker.class_ = cls
