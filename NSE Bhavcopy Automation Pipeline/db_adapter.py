import os
import json
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

def get_connection_string(config_path=None):
    """
    Reads the database configuration and builds the SQLAlchemy connection string.
    If config_path is None, it will look for db_config.json in the same directory.
    """
    # Determine path to db_config.json
    if not config_path:
        base_dir = os.path.dirname(__file__)
        config_path = os.path.join(base_dir, "db_config.json")

    # Load configuration
    with open(config_path, "r") as f:
        config = json.load(f)

    db_type = config.get("db_type")

    if db_type == "sqlite":
        db_path = config["sqlite"]["db_path"]
        return f"sqlite:///{db_path}"

    elif db_type == "mysql":
        mysql_conf = config["mysql"]
        user = mysql_conf["user"]
        password = mysql_conf["password"]
        host = mysql_conf["host"]
        port = mysql_conf["port"]
        database = mysql_conf["database"]
        # Make sure PyMySQL is available in your Lambda layer
        return f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"

    elif db_type == "oracle":
        oracle_conf = config["oracle"]
        user = oracle_conf["user"]
        password = oracle_conf["password"]
        dsn = oracle_conf["dsn"]
        return f"oracle+oracledb://{user}:{password}@{dsn}"

    else:
        raise ValueError(f"Unsupported db_type: {db_type}")


class SQLAlchemyAdapter:
    """
    Wraps SQLAlchemy engine and session creation with fast-fail and health checks.
    Usage:
        conn_str = get_connection_string()
        db = SQLAlchemyAdapter(conn_str)
        session = db.session
    """
    def __init__(self, connection_string):
        # Enable pool_pre_ping to detect stale connections
        # Set a short timeout to fail fast if RDS is unreachable
        self.engine = create_engine(
            connection_string,
            pool_pre_ping=True,
            connect_args={"connect_timeout": 5}
        )
        Session = sessionmaker(bind=self.engine)
        self.session = Session()

    def bulk_insert(self, model_class, objects):
        """
        Bulk-save a list of ORM objects and commit in one transaction.
        """
        try:
            self.session.bulk_save_objects(objects)
            self.session.commit()
            print(f"Inserted {len(objects)} rows into {model_class.__tablename__}")
        except Exception:
            self.session.rollback()
            raise

    def close(self):
        """
        Close the session and dispose the engine.
        """
        self.session.close()
        self.engine.dispose()
