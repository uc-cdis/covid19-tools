from importlib import import_module
from pathlib import Path

__all__ = []
for f in Path(__file__).parent.glob("*.py"):
    if "__" not in f.stem:
        try:
            __all__.append(import_module(f".{f.stem}", __package__))
        except Exception as e:
            print(f"Unable to import module {f.stem} - skipping it. Details: {e}")
del import_module, Path
