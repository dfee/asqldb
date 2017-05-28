from sqlalchemy.orm import Session


class SingletonSession:
    registry = {}

    def __init__(self, *args, bind, **kwargs):
        connection = self.registry[bind][0]
        self._tx = connection.begin_nested()
        self.session = Session(*args, bind=connection, **kwargs)

    def __getattr__(self, name):
        return getattr(self.session, name)

    def close(self):
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


#  from sqlalchemy import create_engine
#  from sqlalchemy.orm import sessionmaker
#  engine = create_engine('postgresql://localhost/asqldemo')
#  maker = sessionmaker(bind=engine, class_=SingletonSession)
#  s = maker()
#  s2 = maker()
#  assert s.bind == s2.bind
