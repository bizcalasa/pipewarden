# pipewarden

A lightweight CLI tool for validating and monitoring ETL pipeline schemas across multiple data sources.

---

## Installation

```bash
pip install pipewarden
```

Or install from source:

```bash
git clone https://github.com/yourname/pipewarden.git && cd pipewarden && pip install .
```

---

## Usage

Define your schema in a `pipewarden.yml` config file, then run:

```bash
pipewarden validate --config pipewarden.yml --source my_database
```

**Example config:**

```yaml
sources:
  my_database:
    type: postgres
    schema: public
    tables:
      - name: orders
        required_columns:
          - id
          - created_at
          - status
```

**Run a full pipeline check:**

```bash
pipewarden check --all --report json
```

Output:

```
✔ orders        — schema valid
✘ customers     — missing column: email
```

---

## Commands

| Command              | Description                          |
|----------------------|--------------------------------------|
| `validate`           | Validate schemas against config      |
| `check`              | Run full pipeline health check       |
| `report`             | Generate a validation report         |

---

## License

This project is licensed under the [MIT License](LICENSE).