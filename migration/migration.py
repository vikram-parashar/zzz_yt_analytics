import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.banners import scrape_banners
from src.agents import scrape_and_load
from src.utils import get_db


def truncate():
    with get_db() as con:
        con.execute("""
        truncate dim_agent
        """)


def dim_agent_update_release_date():
    with get_db() as con:
        con.execute("""
            insert into dim_agent (name,release_date) values
                ('Sunna','2026-2-6'),
                ('Aria','2026-3-4'),
                ('Nangong Yu','2026-3-24'),
                ('Koleda','2024-7-4'),
                ('Ben','2024-7-4'),
                ('Anton','2024-7-4'),
                ('Grace','2024-7-4'),
                ('Billy','2024-7-4'),
                ('Nicole','2024-7-4'),
                ('Anby','2024-7-4'),
                ('Starlight Billy','2026-5-27'),
                ('Nekomata','2024-7-4'),
                ('Banyue','2025-12-17'),
                ('Zhao','2025-12-30'),
                ('Dialyn','2025-11-26'),
                ('Promeia','2026-5-6'),
                ('Cissia','2026-4-15'),
                ('Hugo','2025-5-14'),
                ('Vivian','2025-4-23'),
                ('Qingyi','2024-8-14'),
                ('Zhu Yuan','2024-7-24'),
                ('Jane','2024-9-4'),
                ('Seth','2024-9-4'),
                ('Soldier 11','2025-3-12'),
                ('Seed','2025-9-4'),
                ('Trigger','2025-4-2'),
                ('Orphie & Magus','2025-9-24'),
                ('Soukaku','2024-7-4'),
                ('Miyabi','2024-12-18'),
                ('Yanagi','2024-11-6'),
                ('Harumasa','2024-12-18'),
                ('Lighter','2024-11-27'),
                ('Lucy','2024-7-4'),
                ('Caesar','2024-9-25'),
                ('Piper','2024-7-4'),
                ('Pulchra','2025-3-12'),
                ('Burnice','2024-10-16'),
                ('Manato','2025-10-15'),
                ('Alice','2025-8-6'),
                ('Yuzuha','2025-7-16'),
                ('Lucia','2025-10-15'),
                ('Yidhari','2025-11-5'),
                ('Evelyn','2025-2-12'),
                ('Astra Yao','2025-1-22'),
                ('Soldier 0 Anby','2025-3-12'),
                ('Corin','2024-7-4'),
                ('Ellen','2024-7-4'),
                ('Lycaon','2024-7-4'),
                ('Rina','2024-7-4'),
                ('Ye Shunguang','2025-12-30'),
                ('Yixuan','2025-6-6'),
                ('Pan Yinhu','2025-6-6'),
                ('Ju Fufu','2025-6-25')
            on conflict(name) do update set release_date=excluded.release_date
        """)


def migration3():
    with get_db() as con:
        con.execute("DROP TABLE IF EXISTS dim_patch")
        con.execute("""
            CREATE TABLE dim_patch (
                version      VARCHAR,
                agent_name   VARCHAR,
                banner_start DATE,
                banner_end   DATE,
                PRIMARY KEY (version, agent_name)
            )
        """)


def dim_patch_insert_exclusive():
    with get_db() as con:
        con.execute("""
            insert into dim_patch (version,agent_name,banner_start,banner_end) values
            ('2.5','Astra Yao','2026-1-21','2026-02-5'),
            ('2.5','Anby: Soldier 0','2026-1-21','2026-02-5'),
            ('2.5','Alice','2026-1-21','2026-02-5')
        """)


truncate()
scrape_and_load()
dim_agent_update_release_date()
migration3()
dim_patch_insert_exclusive()
scrape_banners()
