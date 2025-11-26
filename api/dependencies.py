from core.engine import VolGuardHybridUltimate

# Global engine instance
engine: VolGuardHybridUltimate = None

def get_engine() -> VolGuardHybridUltimate:
    return engine

def set_engine(new_engine: VolGuardHybridUltimate):
    global engine
    engine = new_engine
