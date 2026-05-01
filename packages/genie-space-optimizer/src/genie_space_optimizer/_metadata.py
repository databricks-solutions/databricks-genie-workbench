from pathlib import Path

app_name = "genie-space-optimizer"
app_entrypoint = "genie_space_optimizer.backend.app:app"
app_slug = "genie_space_optimizer"
api_prefix = "/api/genie"
dist_dir = Path(__file__).parent / "__dist__"