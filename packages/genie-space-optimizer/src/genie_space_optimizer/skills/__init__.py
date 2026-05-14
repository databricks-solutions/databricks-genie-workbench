"""Plan 4 — skill prompt registry. Each skill_id has its own
``SKILL.md`` file with YAML frontmatter + Markdown body. The body
IS the prompt template; ``{{ slot }}`` placeholders are unchanged
from when the prompts lived as ``XXX_PROMPT = (...)`` constants in
``common.config``.

The loader's job is to keep the existing import surface
(``from common.config import LEVER_1_2_COLUMN_PROMPT``) working
byte-stably while moving the bodies into discoverable per-skill
files."""
