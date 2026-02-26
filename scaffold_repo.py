from pathlib import Path

TREE = {
    "README.md": None,
    "pyproject.toml": None,
    ".env.example": None,
    ".gitignore": None,
    "configs": {
        "app.yaml": None,
        "mappings": {
            "recibos.yaml": None,
            "_template.yaml": None,
        },
        "sql": {
            "oracle": {},
            "redshift": {},
        },
    },
    "src": {
        "etl": {
            "__init__.py": None,
            "config": {
                "settings.py": None,
                "loader.py": None,
            },
            "clients": {
                "oracle_client.py": None,
                "redshift_client.py": None,
                "thona_api_client.py": None,
            },
            "extract": {
                "oracle_extract.py": None,
                "api_extract.py": None,
            },
            "transform": {
                "validators.py": None,
                "normalizers.py": None,
            },
            "load": {
                "redshift_load.py": None,
                "upsert.py": None,
            },
            "logging": {
                "etl_log_repo.py": None,
                "models.py": None,
            },
            "flows": {
                "recibos_flow.py": None,
                "manual_upload_flow.py": None,
            },
            "utils": {
                "time.py": None,
                "files.py": None,
            },
        }
    },
    "tests": {
        "unit": {},
        "integration": {},
    },
    "scripts": {
        "create_redshift_schemas.sql": None,
        "run_local.ps1": None,
    },
}

def create_tree(base: Path, spec: dict) -> None:
    base.mkdir(parents=True, exist_ok=True)
    for name, children in spec.items():
        path = base / name
        if children is None:
            # archivo
            path.parent.mkdir(parents=True, exist_ok=True)
            path.touch(exist_ok=True)
        else:
            # folder
            path.mkdir(parents=True, exist_ok=True)
            create_tree(path, children)

if __name__ == "__main__":
    # Crea la estructura dentro del folder actual: ./my-etl-repo
    root = Path("my-etl-repo")
    create_tree(root, TREE)
    print(f"Estructura creada en: {root.resolve()}")