# Building the documentation

This note is for working on the docs locally. It is not part of the built site.

```bash
pip install -e . -r docs/requirements.txt
sphinx-build -W --keep-going -b html docs docs/_build/html
```

Open `docs/_build/html/index.html` to view the result.

The narrative pages are Markdown, parsed by MyST. Diagrams are written as fenced `mermaid` blocks, which render both in the built site and on GitHub. The API reference is generated from docstrings. Keep docstrings in NumPy style, since warnings fail the build.
