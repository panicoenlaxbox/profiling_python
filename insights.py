import ast
from dataclasses import dataclass
from functools import reduce
from pathlib import Path
from typing import TextIO


@dataclass
class SymbolSummary:
    classes: int = 0
    functions: int = 0


class InsightsHtmlMaker:
    def __init__(self, path: Path, output_path: Path = Path("index.html")):
        self._path = path
        self._output_path = output_path
        self._f: TextIO

    def __enter__(self):
        self._f = open(self._output_path, "w")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._f.close()

    @staticmethod
    def _is_dir_excluded(path: Path) -> bool:
        return any(path.name.endswith(exclude) for exclude in ["__pycache__"])

    @classmethod
    def _get_symbols(cls, path: Path) -> SymbolSummary:
        classes = 0
        functions = 0
        if path.is_dir():
            for file in path.glob("**/*.py"):
                symbols = cls._get_file_symbols(file)
                classes += symbols.classes
                functions += symbols.functions
            return SymbolSummary(classes, functions)
        else:
            return cls._get_file_symbols(path)

    @staticmethod
    def _get_file_symbols(path: Path) -> SymbolSummary:
        with open(path, "r", errors="ignore") as f:
            root_node = ast.parse(f.read(), str(path))
        classes = 0
        functions = 0
        for node in ast.iter_child_nodes(root_node):
            if isinstance(node, ast.ClassDef):
                classes += 1
            elif isinstance(node, ast.FunctionDef):
                functions += 1
            else:
                continue
        return SymbolSummary(classes, functions)

    @classmethod
    def _get_kloc(cls, path: Path) -> int:
        if path.is_dir():
            return cls._get_dir_kloc(path)
        elif path.is_file():
            return cls._get_file_kloc(path)

    @staticmethod
    def _get_file_kloc(path: Path) -> int:
        with open(path, "r", errors="ignore") as f:
            return len([line for line in f.readlines() if line.strip() != ""])

    @classmethod
    def _get_dir_kloc(cls, path: Path) -> int:
        return reduce(lambda x, y: x + y, [cls._get_file_kloc(f) for f in path.glob(f"**/*.py")], 0)

    @classmethod
    def _skip_path(cls, path: Path) -> bool:
        return (path.is_file() and not path.suffix == ".py") or (path.is_dir() and cls._is_dir_excluded(path))

    def _make_html(self, path: Path) -> None:
        for path in path.iterdir():
            if self._skip_path(path):
                continue
            self._f.write("<tr>\n")
            self._f.write(f"<td>{('d' if path.is_dir() else '')}</td>\n")
            self._f.write(f"<td><a href='{path}'>{path}</a></td>\n")
            self._f.write(f"<td>{self._get_kloc(path)}</td>\n")
            symbol_summary = self._get_symbols(path)
            self._f.write(f"<td>{symbol_summary.classes}</td>\n")
            self._f.write(f"<td>{symbol_summary.functions}</td>\n")
            self._f.write("</tr>\n")
            if path.is_dir():
                self._make_html(path)

    def make_html(self):
        self._f.write("<html>\n")
        self._f.write("<body>\n")
        self._f.write(f"<h1>{self._path.name}</h1>\n")
        self._f.write("<table>\n")
        self._f.write("<thead>\n")
        self._f.write("<tr>\n")
        self._f.write("<th></th>\n")
        self._f.write("<th>path</th>\n")
        self._f.write("<th>kloc</th>\n")
        self._f.write("<th>classes</th>\n")
        self._f.write("<th>functions</th>\n")
        self._f.write("</thead>\n")
        self._f.write("<tbody>\n")
        self._f.write("<tr>\n")
        self._f.write(f"<td>d</td>\n")
        self._f.write(f"<td><a href='{self._path}'>{self._path}</a></td>\n")
        self._f.write(f"<td>{self._get_dir_kloc(self._path)}</td>\n")
        symbol_summary = self._get_symbols(self._path)
        self._f.write(f"<td>{symbol_summary.classes}</td>\n")
        self._f.write(f"<td>{symbol_summary.functions}</td>\n")
        self._f.write("</tr>\n")
        self._make_html(self._path)
        self._f.write("</tbody>\n")
        self._f.write("</table>\n")
        self._f.write("</body>\n")
        self._f.write("</html>\n")


if __name__ == "__main__":
    path = Path(r"YOUR_DIRECTORY_GOES_HERE")
    with InsightsHtmlMaker(path) as html:
        html.make_html()