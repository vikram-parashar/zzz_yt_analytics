from utils import get_db


def migration1():
    with get_db() as con:
        con.execute("""
            alter table dim_agent drop column release_version
        """)


def migration2():
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
                ('Billy - Starlight','2026-5-27'),
                ('Nekomata','2024-7-4'),
                ('Norma',null),
                ('Velina',null),
                ('Banyue','2025-12-17'),
                ('Zhao','2025-12-30'),
                ('Dialyn','2025-11-26'),
                ('Promeia','2026-5-6'),
                ('Cissia','2026-4-15'),
                ('Hugo','2025-5-14'),
                ('Vivian','2025-4-23'),
                ('Qingyi','2024-8-14'),
                ('Zhu Yuan','2024-7-24'),
                ('Jane Doe','2024-9-4'),
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
                ('Pyrois',null),
                ('Sunbringer',null),
                ('Remielle',null),
                ('Sigrid',null),
                ('Roxy',null),
                ('Claret',null),
                ('The Storyteller',null),
                ('Phoenix',null),
                ('Severian',null),
                ('Anby: Soldier 0',null),
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


migration1()
migration2()
