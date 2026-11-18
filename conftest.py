"""El alta de --repo vive en tests/conftest.py.

Así funciona aunque se copie solo tests/ (sin este fichero) a otro
repositorio. Si este conftest de la raíz también la registrara, pytest
fallaría con «option names already added» cuando carga los dos.
"""
