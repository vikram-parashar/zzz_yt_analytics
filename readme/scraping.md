# Agent & Banner Sources

## Agents

Scraped from [LootBar ZZZ Character List](https://www.lootbar.com/blog/en/zenless-zone-zero-character-list.html).

Parses "All Playable Characters" section → extracts name, rank (S/A), attribute, speciality, faction, and image. Also captures upcoming characters from tables above that section. Upserted into `dim_agent` (keyed on `name`).

## Aliases

Manual mapping in `data/aliases.json`. Each agent has search-friendly aliases (e.g. `"Zhu Yuan": ["zhu yuan", "zhuyuan"]`, `"Soldier 0 Anby": ["soldier 0", "soldier0", "sanby"]`). Loaded into `bridge_agent_alias` with word-boundary regex patterns for matching.

## Banners

Scraped from [GameRant ZZZ Banner History](https://gamerant.com/zenless-zone-zero-zzz-current-next-past-banner-history-schedule/).

Parses current banner table + "ZZZ Banner History" section → extracts version, agent name, banner start/end dates. Filters out bangboo/standard/engine/select rows. Upserted into `dim_patch`.
