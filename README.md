# deltav-flow

A small Rust project providing a lightweight data pipeline scheduler and utilities.

See the `nanoservices/core/README.md` for details on the core crate, including the Prometheus metrics exporter example.

## Frontend

This repository includes mock frontends for visualizing the DeltaV Flow pipeline scheduler:

- `frontend/` - Static HTML/CSS/JS version
- `frontend/htmx/` - Enhanced version using HTMX for dynamic interactions

To run the HTMX frontend:
```bash
cd frontend/htmx
python server.py
```

Then navigate to http://localhost:8000 in your browser.